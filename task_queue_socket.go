package conveyor

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

var (
	taskRegistry          = make(map[string]func() ITask)
	errInvalidMagicNumber = errors.New("invalid magic number received")
	magicNumber           = []byte{0x1A, 0x2B, 0x3C, 0x4D} // Define as byte slice directly
)

var _ TaskQueue = (*SocketTaskQueue)(nil)

// SocketQueueOptions holds configuration options for SocketTaskQueue.
type SocketQueueOptions struct {
	TaskChanBuffer        int           // Buffer size for the internal task channel
	WriteChanBuffer       int           // Buffer size for the outgoing message channel
	ReadChanBuffer        int           // Buffer size for the incoming message channel
	ReconnectInitialDelay time.Duration // Initial delay before the first reconnect attempt
	ReconnectMaxDelay     time.Duration // Maximum delay between reconnect attempts
	ReconnectFactor       float64       // Multiplier for exponential backoff
	ReconnectJitter       float64       // Jitter factor to randomize reconnect delays (0.0 to 1.0)
}

// DefaultSocketQueueOptions returns a new SocketQueueOptions with default values.
func DefaultSocketQueueOptions() *SocketQueueOptions {
	return &SocketQueueOptions{
		TaskChanBuffer:        1000,
		WriteChanBuffer:       1000,
		ReadChanBuffer:        1000,
		ReconnectInitialDelay: 1 * time.Second,
		ReconnectMaxDelay:     60 * time.Second,
		ReconnectFactor:       2.0,
		ReconnectJitter:       0.1, // 10% jitter
	}
}

// RegisterTaskType registers a task type with the socket task queue for deserialization.
// The taskFactory function should return a new, empty instance of the task type.
func RegisterTaskType(taskType string, taskFactory func() ITask) {
	taskRegistry[taskType] = taskFactory
	slog.Debug("Registered task type", "type", taskType)
}

// SocketTaskQueue implements the TaskQueue interface using network sockets.
type SocketTaskQueue struct {
	listener    net.Listener          // Server mode: listens for incoming connections
	clientConn  net.Conn              // Client mode: the single connection to the server
	activeConns map[net.Conn]struct{} // Server mode: tracks active client connections
	addr        string
	network     string
	mode        string     // "server" or "client"
	mu          sync.Mutex // Protects clientConn, activeConns, closed, listener fields
	cond        *sync.Cond // Condition variable to signal connection status changes (primarily for client reconnect)
	closed      bool

	// taskChan is the primary channel for task exchange.
	// - In server mode: It receives deserialized tasks from the messageProcessor (originating from any connected client via readChan).
	//                  The Pop() method reads from this channel.
	// - In client mode: The Push() method sends marshaled tasks (as []byte) to writeChan, which are then sent over the socket.
	//                  This channel is NOT directly used by Push/Pop in client mode for task objects.
	taskChan chan ITask

	// writeChan buffers marshaled messages (task data wrapped in SocketMessage, then marshaled to JSON bytes)
	// waiting to be sent over the network connection(s).
	// - In client mode: Push() sends marshaled message bytes here. The writerLoop reads from this channel and writes to the clientConn.
	// - In server mode: This channel is used by the writerLoop associated with EACH client connection.
	//                   Currently, the server only receives tasks (Pop), it doesn't Push tasks back, so this channel is less utilized in server mode's core logic but necessary for the writerLoop structure.
	writeChan chan []byte

	// readChan buffers raw message bytes received from the network connection(s) before processing.
	// - The readerLoop associated with each connection reads raw bytes, frames them into messages (using readMessage), and sends the message bytes here.
	// - The single messageProcessor goroutine reads from this channel, deserializes the message bytes into ITask objects, and sends them to taskChan.
	readChan chan []byte

	// WaitGroup to wait for goroutines to finish
	wg sync.WaitGroup

	// Context for managing goroutine lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// Configuration options
	opts *SocketQueueOptions
}

// NewSocketTaskQueue creates a new SocketTaskQueue with optional configurations.
// mode should be "server" or "client".
// network should be "tcp" or "unix".
// addr is the address to listen on (server) or connect to (client).
// If opts is nil, default options are used.
func NewSocketTaskQueue(mode, network, addr string, opts *SocketQueueOptions) (*SocketTaskQueue, error) {
	if opts == nil {
		opts = DefaultSocketQueueOptions()
	}
	// Validate options (optional but recommended)
	if opts.ReconnectJitter < 0 || opts.ReconnectJitter > 1 {
		return nil, fmt.Errorf("invalid ReconnectJitter value: %f, must be between 0.0 and 1.0", opts.ReconnectJitter)
	}
	if opts.ReconnectFactor <= 1 {
		return nil, fmt.Errorf("invalid ReconnectFactor value: %f, must be greater than 1.0", opts.ReconnectFactor)
	}
	if opts.ReconnectInitialDelay <= 0 || opts.ReconnectMaxDelay <= 0 || opts.ReconnectMaxDelay < opts.ReconnectInitialDelay {
		return nil, fmt.Errorf("invalid reconnect delay values: Initial=%v, Max=%v", opts.ReconnectInitialDelay, opts.ReconnectMaxDelay)
	}
	if opts.TaskChanBuffer < 0 || opts.WriteChanBuffer < 0 || opts.ReadChanBuffer < 0 {
		return nil, fmt.Errorf("channel buffer sizes cannot be negative: Task=%d, Write=%d, Read=%d", opts.TaskChanBuffer, opts.WriteChanBuffer, opts.ReadChanBuffer)
	}

	ctx, cancel := context.WithCancel(context.Background())

	q := &SocketTaskQueue{
		addr:        addr,
		network:     network,
		mode:        mode,
		activeConns: make(map[net.Conn]struct{}),             // Initialize for server mode
		taskChan:    make(chan ITask, opts.TaskChanBuffer),   // Use option
		writeChan:   make(chan []byte, opts.WriteChanBuffer), // Use option
		readChan:    make(chan []byte, opts.ReadChanBuffer),  // Use option
		ctx:         ctx,
		cancel:      cancel,
		opts:        opts, // Store options
	}

	// Initialize condition variable for both modes
	q.cond = sync.NewCond(&q.mu)

	var err error
	switch mode {
	case "server":
		if network == "tcp" {
			q.listener, err = net.Listen("tcp", addr)
			if err != nil {
				return nil, fmt.Errorf("failed to listen on tcp %s: %w", addr, err)
			}
			slog.Info("Server listening on tcp", "addr", q.listener.Addr().String())
		} else if network == "unix" {
			// Check if socket file exists and remove it
			if _, err := os.Stat(addr); err == nil {
				slog.Info("Server removing existing unix socket file", "addr", addr)
				if removeErr := os.Remove(addr); removeErr != nil {
					return nil, fmt.Errorf("failed to remove existing unix socket file %s: %w", addr, removeErr)
				}
			}
			q.listener, err = net.Listen("unix", addr)
			if err != nil {
				return nil, fmt.Errorf("failed to listen on unix %s: %w", addr, err)
			}
			slog.Info("Server listening on unix", "addr", addr)
		} else {
			return nil, fmt.Errorf("server mode unsupported network type: %s", network)
		}
		go q.acceptConnections() // Start accepting connections
		// Start the goroutine to process received messages and put them into taskChan
		q.wg.Add(1)
		go q.messageProcessor()

	case "client":
		slog.Info("Client connecting to", "network", network, "addr", addr)
		// Seed random for jitter in reconnection - No longer needed since Go 1.20

		// Attempt initial connection
		conn, err := net.Dial(network, addr)
		if err != nil {
			slog.Error("Initial client connection failed", "error", err)
			// Start reconnect goroutine even if initial connection fails
			go q.reconnectClient()
			// Return error to the caller if initial connection fails
			return nil, fmt.Errorf("initial client connection failed: %w", err)
		} else {
			q.mu.Lock()
			q.clientConn = conn // Use clientConn for client mode
			q.mu.Unlock()
			slog.Info("Client connected successfully", "remote_addr", q.clientConn.RemoteAddr()) // Use clientConn
			// Signal that a connection is available
			q.cond.Broadcast()
			// Start reader and writer goroutines for the new connection
			// Start reader and writer goroutines for the new connection.
			// This function now blocks until both loops signal they have started.
			q.startConnectionGoroutines(conn)
		}

		// Always start client reconnection logic in a goroutine
		go q.reconnectClient()

		// Start the goroutine to process received messages and put them into taskChan
		q.wg.Add(1)
		go q.messageProcessor()

		// Return the queue instance on successful initial connection
		return q, nil
	default:
		return nil, fmt.Errorf("unsupported mode: %s, must be 'server' or 'client'", mode)
	}

	return q, nil
}

// acceptConnections accepts incoming socket connections.
func (q *SocketTaskQueue) acceptConnections() {
	for {
		conn, err := q.listener.Accept()
		if err != nil {
			if !q.isClosed() {
				slog.Error("Failed to accept connection", "error", err)
			}
			return // Stop accepting if listener is closed or error occurs
		}

		q.mu.Lock()
		// Add the new connection to the map of active connections
		q.activeConns[conn] = struct{}{}
		slog.Info("Accepted new connection", "remote_addr", conn.RemoteAddr(), "total_connections", len(q.activeConns))
		q.mu.Unlock()

		// Start reader and writer goroutines for the new connection
		// Start reader and writer goroutines for the new connection.
		// This function now blocks until both loops signal they have started.
		q.startConnectionGoroutines(conn)
	}
}

// reconnectClient attempts to reconnect to the server (client mode) with exponential backoff using configured options.
func (q *SocketTaskQueue) reconnectClient() {
	// Use configured options
	initialDelay := q.opts.ReconnectInitialDelay
	maxDelay := q.opts.ReconnectMaxDelay
	factor := q.opts.ReconnectFactor
	jitter := q.opts.ReconnectJitter

	currentDelay := initialDelay

	slog.Info("Client reconnect parameters",
		"initialDelay", initialDelay,
		"maxDelay", maxDelay,
		"factor", factor,
		"jitter", jitter)

	for {
		q.mu.Lock()
		// Use a loop for the condition check, standard practice with sync.Cond
		for q.clientConn != nil && !q.closed {
			slog.Debug("Client connection exists, waiting for disconnection signal...")
			q.cond.Wait() // Wait for handleConnectionClose or Close to signal
		}
		closed := q.closed
		// At this point, q.clientConn is nil or q.closed is true
		q.mu.Unlock()

		if closed {
			slog.Info("Client reconnection goroutine exiting: queue is closed")
			return // Exit if the queue is closed
		}

		// ---- Connection is confirmed disconnected (q.clientConn == nil), attempt reconnect ----

		slog.Info("Client attempting to reconnect", "network", q.network, "addr", q.addr, "delay", currentDelay)
		newConn, err := net.Dial(q.network, q.addr)
		if err != nil {
			slog.Error("Client reconnection failed", "error", err)

			// Calculate next delay with backoff and jitter
			sleepDuration := currentDelay
			// Add jitter: random value between -jitter*currentDelay and +jitter*currentDelay
			sleepDuration += time.Duration((rand.Float64() - 0.5) * 2.0 * jitter * float64(currentDelay)) // Use Float64 for better distribution

			// Ensure sleepDuration is not negative
			if sleepDuration < 0 {
				sleepDuration = 0
			}

			// Use time.Timer for cancellable sleep
			timer := time.NewTimer(sleepDuration)
			select {
			case <-timer.C:
				// Timer expired, continue to next attempt
			case <-q.ctx.Done():
				slog.Info("Client reconnection context cancelled during backoff sleep, exiting")
				// Attempt to stop the timer and drain if necessary
				if !timer.Stop() {
					// If Stop returns false, the timer already fired and its channel might need draining.
					// Select avoids blocking if the channel is already empty.
					select {
					case <-timer.C:
					default:
					}
				}
				return // Queue is closing
			}

			// Increase delay, cap at maxDelay
			currentDelay = time.Duration(float64(currentDelay) * factor)
			if currentDelay > maxDelay {
				currentDelay = maxDelay
			}

			continue // Retry connection
		}

		q.mu.Lock()
		// Set the new connection as active for the client.
		// The old handle will be closed by its associated reader/writer loops when they exit.
		q.clientConn = newConn // Use clientConn for client mode
		q.mu.Unlock()

		// Let the old reader/writer loops handle closing their own connection handles via handleConnectionClose.
		// No need to explicitly close oldConn here. If oldConn exists and is different from newConn,
		// its associated goroutines should have already exited or will exit due to the original disconnection,
		// triggering handleConnectionClose(oldConn).

		slog.Info("Client reconnected successfully", "remote_addr", newConn.RemoteAddr())

		// Start reader and writer goroutines IMMEDIATELY for the new connection.
		// This function now blocks until both loops signal they have started.
		q.startConnectionGoroutines(newConn)

		// Signal that a connection is potentially available
		// (loops have confirmed started)
		q.cond.Broadcast()

		// Reset delay on successful connection attempt (loops might still fail later)
		currentDelay = initialDelay
	}
}

// Push adds a task to the queue by sending it over the socket.
// It marshals the task and sends the message bytes to the internal write channel.
func (q *SocketTaskQueue) Push(ctx context.Context, task ITask) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errors.New("Push: queue is closed")
	}
	q.mu.Unlock()

	// Marshal the task data into json.RawMessage
	taskDataBytes, err := json.Marshal(task)
	if err != nil {
		slog.Error("Failed to marshal task data", "identifier", task.GetIdentify(), "error", err)
		return fmt.Errorf("Push: failed to marshal task data for task %s: %w", task.GetIdentify(), err)
	}

	// Get the task type name, handling pointers
	taskType := fmt.Sprintf("%T", task.GetType())
	if taskType[0] == '*' {
		taskType = taskType[1:] // Remove the leading '*' for pointer types
	}

	// Create a SocketMessage
	message := SocketMessage{
		TaskType: taskType,
		TaskData: json.RawMessage(taskDataBytes),
	}

	// Marshal the SocketMessage
	messageBytes, err := json.Marshal(message)
	if err != nil {
		slog.Error("Failed to marshal socket message", "identifier", task.GetIdentify(), "error", err)
		return fmt.Errorf("Push: failed to marshal socket message for task %s: %w", task.GetIdentify(), err)
	}

	// Send the message bytes to the write channel
	select {
	case q.writeChan <- messageBytes:
		slog.Debug("Task marshaled and sent to writeChan", "identifier", task.GetIdentify())
		return nil
	case <-ctx.Done():
		slog.Debug("Push: Context done while sending to writeChan", "error", ctx.Err())
		return fmt.Errorf("Push: context done while sending to writeChan: %w", ctx.Err())
	case <-q.ctx.Done():
		slog.Debug("Push: Queue context done while sending to writeChan, queue is closing")
		return errors.New("Push: queue is closing")
	}
}

// Pop retrieves a task from the queue by reading from the internal task channel.
// It waits for a task to be available, respecting the context deadline.
func (q *SocketTaskQueue) Pop(ctx context.Context) (ITask, error) {
	var zero ITask // Declare zero value for ITask

	select {
	case task, ok := <-q.taskChan:
		if !ok {
			slog.Debug("taskChan closed, Pop returning closed error")
			return zero, errors.New("Pop: queue is closed")
		}
		slog.Debug("Task received from taskChan", "identifier", task.GetIdentify())
		return task, nil
	case <-ctx.Done():
		slog.Debug("Pop: Context done while waiting for task", "error", ctx.Err())
		return zero, fmt.Errorf("Pop: context done while waiting for task: %w", ctx.Err())
	case <-q.ctx.Done():
		slog.Debug("Pop: Queue context done while waiting for task, queue is closing")
		return zero, errors.New("Pop: queue is closing")
	}
}

// Len returns the current number of tasks in the queue.
// For a socket-based queue, this concept is tricky. It could represent
// the number of tasks waiting to be sent/received, but that requires
// buffering and a more complex protocol. For this simple implementation,
// we'll return 0 as we don't buffer tasks internally.
func (q *SocketTaskQueue) Len() int {
	return 0
}

// closeConnectionHandles closes active connection handles based on the mode.
// Should be called after setting q.closed = true and canceling the context.
func (q *SocketTaskQueue) closeConnectionHandles() []error {
	q.mu.Lock()
	var clientConnToClose net.Conn
	var serverConnsToClose []net.Conn
	if q.mode == "client" {
		clientConnToClose = q.clientConn
		q.clientConn = nil // Clear the active client connection
	} else { // server mode
		serverConnsToClose = make([]net.Conn, 0, len(q.activeConns))
		for conn := range q.activeConns {
			serverConnsToClose = append(serverConnsToClose, conn)
		}
		q.activeConns = make(map[net.Conn]struct{}) // Clear the active server connections map
	}
	q.mu.Unlock() // Unlock before closing handles

	var connErrs []error
	if q.mode == "client" && clientConnToClose != nil {
		slog.Info("Closing captured client connection handle during Close", "local_addr", clientConnToClose.LocalAddr(), "remote_addr", clientConnToClose.RemoteAddr())
		if closeErr := clientConnToClose.Close(); closeErr != nil {
			// Simplify error check: net.ErrClosed should cover most "use of closed network connection" scenarios.
			if !errors.Is(closeErr, net.ErrClosed) {
				connErrs = append(connErrs, fmt.Errorf("failed to close client connection handle: %w", closeErr))
				slog.Error("Error closing client connection handle", "error", closeErr)
			} else {
				slog.Debug("Client connection handle already closed, ignoring error in Close", "local_addr", clientConnToClose.LocalAddr(), "remote_addr", clientConnToClose.RemoteAddr())
			}
		}
	} else if q.mode == "server" {
		slog.Info("Closing captured server connection handles during Close", "count", len(serverConnsToClose))
		for _, conn := range serverConnsToClose {
			if closeErr := conn.Close(); closeErr != nil {
				// Simplify error check: net.ErrClosed should cover most "use of closed network connection" scenarios.
				if !errors.Is(closeErr, net.ErrClosed) {
					connErrs = append(connErrs, fmt.Errorf("failed to close server connection handle (%s): %w", conn.RemoteAddr(), closeErr))
					slog.Error("Error closing server connection handle", "remote_addr", conn.RemoteAddr(), "error", closeErr)
				} else {
					slog.Debug("Server connection handle already closed, ignoring error in Close", "remote_addr", conn.RemoteAddr())
				}
			}
		}
	}
	return connErrs
}

// Close closes the queue, stopping all goroutines and connections.
func (q *SocketTaskQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.mu.Unlock()

	slog.Info("Closing socket task queue")

	// Cancel context to signal goroutines to exit
	q.cancel()

	// Close the listener (if in server mode)
	var listenerErr error
	if q.listener != nil {
		slog.Info("Closing listener")
		if closeErr := q.listener.Close(); closeErr != nil {
			listenerErr = fmt.Errorf("failed to close listener: %w", closeErr)
		}
	}

	// Close active connection handles
	connErrs := q.closeConnectionHandles() // Call the helper function

	// Combine listener and connection errors
	finalErr := errors.Join(listenerErr, errors.Join(connErrs...)) // Combine all collected non-nil errors

	// Close channels
	close(q.writeChan)
	close(q.readChan)
	slog.Debug("readChan closed by Close")
	close(q.taskChan)
	slog.Debug("taskChan closed by Close")

	// Wait for all goroutines to finish
	q.wg.Wait()

	slog.Info("Socket task queue closed successfully")
	return finalErr // Return the combined error
}

// startConnectionGoroutines starts the reader and writer goroutines for a given connection
// and waits for them to signal they have started before returning.
func (q *SocketTaskQueue) startConnectionGoroutines(conn net.Conn) {
	var startWg sync.WaitGroup
	startWg.Add(2) // Expect signals from reader and writer

	q.wg.Add(2)                     // Add 2 to the main WaitGroup for overall lifecycle management
	go q.readerLoop(conn, &startWg) // Pass the internal startWg
	go q.writerLoop(conn, &startWg) // Pass the internal startWg

	// Wait here until both reader and writer have called startWg.Done()
	startWg.Wait()
	slog.Debug("Reader and Writer loops confirmed started", "remote_addr", conn.RemoteAddr())
}

// messageProcessor reads raw message bytes from readChan, deserializes them,
// and sends the resulting ITask to taskChan.
func (q *SocketTaskQueue) messageProcessor() {
	defer q.wg.Done()
	slog.Info("Message processor started")

	for {
		select {
		case messageBytes, ok := <-q.readChan:
			if !ok {
				slog.Debug("readChan closed, message processor exiting")
				return // readChan is closed, exit
			}

			// Unmarshal the SocketMessage with stricter checking
			var message SocketMessage
			// It's better to handle JSON decoding errors here in the messageProcessor
			// as the readerLoop just deals with raw bytes.
			decoder := json.NewDecoder(bytes.NewReader(messageBytes))
			decoder.DisallowUnknownFields() // Disallow unknown fields in the SocketMessage itself
			if err := decoder.Decode(&message); err != nil {
				slog.Error("Failed to unmarshal socket message in message processor (strict)", "error", err, "raw_bytes", string(messageBytes))
				// If the core SocketMessage structure is invalid, we can't proceed with this message.
				continue // Skip this message and continue processing
			}

			// Use the message.TaskType to determine the concrete task type
			// and unmarshal message.TaskData into an instance of that type.
			taskFactory, ok := taskRegistry[message.TaskType]
			if !ok {
				slog.Error("Unknown task type received in message processor", "type", message.TaskType)
				// Continue processing next message on unknown type
				continue
			}

			task := taskFactory()
			// Unmarshal the task data into the concrete type with stricter checking
			taskDecoder := json.NewDecoder(bytes.NewReader(message.TaskData))
			taskDecoder.DisallowUnknownFields() // Disallow unknown fields in the task data
			if err := taskDecoder.Decode(task); err != nil {
				slog.Error("Failed to unmarshal task data into concrete type in message processor (strict)", "type", message.TaskType, "error", err)
				// Continue processing next message on unmarshal error
				continue
			}

			slog.Debug("Task deserialized and sending to taskChan", "type", message.TaskType, "identifier", task.GetIdentify())

			// Send the deserialized task to the taskChan
			select {
			case q.taskChan <- task:
				slog.Debug("Task sent to taskChan", "identifier", task.GetIdentify())
			case <-q.ctx.Done():
				slog.Debug("Message processor context done while sending to taskChan, exiting")
				return
			}

		case <-q.ctx.Done():
			slog.Debug("Message processor context done, exiting")
			return
		}
	}
}

// readerLoop reads messages from the connection and sends them to the readChan.
func (q *SocketTaskQueue) readerLoop(conn net.Conn, startWg *sync.WaitGroup) {
	defer q.wg.Done() // For the main lifecycle WaitGroup
	defer func() {
		slog.Info("Reader loop exiting", "remote_addr", conn.RemoteAddr())
		// When reader loop exits, it means the connection is broken or closed.
		// We should close the connection and signal for reconnection (if client).
		q.handleConnectionClose(conn)
	}()

	slog.Info("Reader loop started", "remote_addr", conn.RemoteAddr())
	slog.Debug("Reader loop running for connection", "local_addr", conn.LocalAddr(), "remote_addr", conn.RemoteAddr()) // Add detailed log

	// Signal that the reader loop has started
	startWg.Done()

	for {
		// Check if the queue is closed or context is done
		select {
		case <-q.ctx.Done():
			slog.Debug("Reader loop context done, exiting")
			return
		default:
			// Continue reading
		}

		messageBytes, err := q.readMessage(conn)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Read timeout, continue loop to check context/closed status
				continue
			}
			if err == io.EOF {
				slog.Info("Connection closed by remote peer during read", "remote_addr", conn.RemoteAddr())
				return // Exit loop on EOF
			} // End of EOF check

			// Check for our custom invalid magic number error
			if errors.Is(err, errInvalidMagicNumber) {
				slog.Warn("Received invalid magic number, skipping message", "remote_addr", conn.RemoteAddr())
				continue // Skip this message and continue reading
			}
			// Check for other potentially recoverable errors (e.g., temporary network issues?)
			// For now, treat other errors as fatal for this connection.
			slog.Error("Failed to read message in reader loop", "error", err, "remote_addr", conn.RemoteAddr())
			return // Exit loop on other fatal errors
		}

		// Send the received message bytes to the readChan
		select {
		case q.readChan <- messageBytes:
			slog.Debug("Message read and sent to readChan", "remote_addr", conn.RemoteAddr(), "size", len(messageBytes))
		case <-q.ctx.Done():
			slog.Debug("Reader loop context done while sending to readChan, exiting")
			return
		}
	}
}

// writerLoop reads messages from the writeChan and writes them to the connection.
func (q *SocketTaskQueue) writerLoop(conn net.Conn, startWg *sync.WaitGroup) {
	defer q.wg.Done() // For the main lifecycle WaitGroup
	defer func() {
		slog.Info("Writer loop exiting", "remote_addr", conn.RemoteAddr())
		// When writer loop exits, it means the connection is broken or closed.
		// We should close the connection and signal for reconnection (if client).
		q.handleConnectionClose(conn)
	}()

	slog.Info("Writer loop started", "remote_addr", conn.RemoteAddr())
	slog.Debug("Writer loop running for connection", "local_addr", conn.LocalAddr(), "remote_addr", conn.RemoteAddr()) // Add detailed log

	// Signal that the writer loop has started
	startWg.Done()

	for {
		select {
		case messageBytes, ok := <-q.writeChan:
			if !ok {
				slog.Debug("writeChan closed, writer loop exiting")
				return // writeChan is closed, exit
			}
			if err := q.writeMessage(conn, messageBytes); err != nil {
				slog.Error("Failed to write message in writer loop", "error", err, "remote_addr", conn.RemoteAddr())
				return // Exit loop on write error
			}
			slog.Debug("Message written to connection", "remote_addr", conn.RemoteAddr(), "size", len(messageBytes))
		case <-q.ctx.Done():
			slog.Debug("Writer loop context done, exiting")
			return
		}
	}
}

// handleConnectionClose is called when a reader or writer loop exits due to error or closure.
// It ensures the specific connection handle used by that loop is closed,
// and if it was the active connection, it clears the reference and signals.
func (q *SocketTaskQueue) handleConnectionClose(conn net.Conn) {
	// Get addresses before closing, as they might become unavailable after.
	connRemoteAddr := conn.RemoteAddr()
	connLocalAddr := conn.LocalAddr()

	// Close the specific connection handle associated with the exiting goroutine.
	// Do this outside the lock.
	slog.Debug("Closing connection handle in handleConnectionClose", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr)
	closeErr := conn.Close() // Close the handle passed by the exiting goroutine
	if closeErr != nil {
		// Log error but continue, as we still need to update internal state.
		// Use Warn level as it's potentially problematic but might be expected if already closed.
		slog.Warn("Error closing connection handle in handleConnectionClose", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr, "error", closeErr)
	}

	// Now, update the queue's state based on the mode.
	q.mu.Lock()
	if q.mode == "client" {
		// If this was the active client connection, clear it and signal.
		if q.clientConn == conn {
			slog.Info("Client active connection closed, clearing reference.", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr)
			q.clientConn = nil
			q.cond.Broadcast() // Signal connection loss for reconnection logic
		} else {
			slog.Debug("Client non-active connection handle closed.", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr)
		}
	} else { // server mode
		// Remove the connection from the active connections map.
		if _, ok := q.activeConns[conn]; ok {
			delete(q.activeConns, conn)
			slog.Info("Server connection closed, removing from active set.", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr, "remaining_connections", len(q.activeConns))
		} else {
			// This might happen if the connection was already removed by Close() or another concurrent close.
			slog.Debug("Server connection handle closed, but not found in active set (already removed?).", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr)
		}
		// NOTE: We no longer close taskChan here in server mode.
		// taskChan should only be closed when the entire queue is closed via Close().
	}
	q.mu.Unlock()
}

func (q *SocketTaskQueue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// SocketMessage represents a message sent over the socket.
type SocketMessage struct {
	TaskType string          `json:"task_type"` // String representation of the task type
	TaskData json.RawMessage `json:"task_data"` // Raw JSON bytes of the task data
}

// readMessage reads a length-prefixed message with a magic number from the connection.
func (q *SocketTaskQueue) readMessage(conn net.Conn) ([]byte, error) {
	// Read the magic number
	receivedMagic := make([]byte, len(magicNumber)) // Use len(magicNumber)
	if _, err := io.ReadFull(conn, receivedMagic); err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}
	// Verify the magic number
	if !bytes.Equal(receivedMagic, magicNumber) { // Use magicNumber directly
		return nil, errInvalidMagicNumber
	}

	// Read the length prefix (uint32, LittleEndian)
	var messageLength uint32
	if err := binary.Read(conn, binary.LittleEndian, &messageLength); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	// Basic sanity check for message length (optional, but good practice)
	// Adjust the limit based on expected maximum message size.
	const maxMessageSize = 10 * 1024 * 1024 // Example: 10MB limit
	if messageLength == 0 {
		return nil, errors.New("received message with zero length")
	}
	if messageLength > maxMessageSize {
		return nil, fmt.Errorf("received message length %d exceeds maximum allowed size %d", messageLength, maxMessageSize)
	}

	// Read the message payload
	message := make([]byte, messageLength)
	if _, err := io.ReadFull(conn, message); err != nil {
		return nil, fmt.Errorf("failed to read message payload: %w", err)
	}

	return message, nil
}

// writeMessage writes a length-prefixed message with a magic number to the connection.
func (q *SocketTaskQueue) writeMessage(conn net.Conn, message []byte) error {
	messageLength := uint32(len(message))

	// Create a buffer for the header (magic number + length)
	header := make([]byte, len(magicNumber)+4)

	// Write magic number to buffer
	copy(header[:len(magicNumber)], magicNumber) // Use magicNumber directly

	// Write length to buffer (LittleEndian)
	binary.LittleEndian.PutUint32(header[len(magicNumber):], messageLength)

	// Write the header
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("failed to write message header: %w", err)
	}

	// Write the message payload
	if _, err := conn.Write(message); err != nil {
		return fmt.Errorf("failed to write message payload: %w", err)
	}

	return nil
}
