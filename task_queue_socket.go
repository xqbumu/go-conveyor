package conveyor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var taskRegistry = make(map[string]func() ITask)

// RegisterTaskType registers a task type with the socket task queue for deserialization.
// The taskFactory function should return a new, empty instance of the task type.
func RegisterTaskType(taskType string, taskFactory func() ITask) {
	taskRegistry[taskType] = taskFactory
	slog.Debug("Registered task type", "type", taskType)
}

// SocketTaskQueue implements the TaskQueue interface using network sockets.
type SocketTaskQueue struct {
	listener net.Listener
	conn     net.Conn
	addr     string
	network  string
	mode     string // "server" or "client"
	mu       sync.Mutex
	cond     *sync.Cond // Condition variable to signal connection status changes
	closed   bool

	// Channels for buffering tasks
	// In server mode, this is the channel for tasks received from the client.
	readChanOnce sync.Once // Ensures readChan is closed only once
	taskChanOnce sync.Once // Ensures taskChan is closed only once
	// In client mode, this is the channel for tasks to be sent to the server.
	taskChan chan ITask

	// Channel for messages to be sent over the socket
	// This is used internally by the writer goroutine.
	writeChan chan []byte

	// Channel for received messages from the socket
	// This is used internally by the reader goroutine.
	readChan chan []byte

	// WaitGroup to wait for goroutines to finish
	wg sync.WaitGroup

	// Context for managing goroutine lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSocketTaskQueue creates a new SocketTaskQueue.
// mode should be "server" or "client".
// network should be "tcp" or "unix".
// addr is the address to listen on (server) or connect to (client).
func NewSocketTaskQueue(mode, network, addr string) (*SocketTaskQueue, error) {
	ctx, cancel := context.WithCancel(context.Background())

	q := &SocketTaskQueue{
		addr:      addr,
		network:   network,
		mode:      mode,
		taskChan:  make(chan ITask, 1000), // Buffered channel for tasks
		writeChan: make(chan []byte, 1000), // Buffered channel for messages to write
		readChan:  make(chan []byte, 1000), // Buffered channel for messages read
		ctx:       ctx,
		cancel:    cancel,
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
		// Seed random for jitter in reconnection
		rand.Seed(time.Now().UnixNano())

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
			q.conn = conn
			q.mu.Unlock()
			slog.Info("Client connected successfully", "remote_addr", q.conn.RemoteAddr())
			// Signal that a connection is available
			q.cond.Broadcast()
			// Start reader and writer goroutines for the new connection
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

	// Start the goroutine to process received messages and put them into taskChan for server mode as well
	if mode == "server" {
		q.wg.Add(1)
		go q.messageProcessor()
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
		if q.conn != nil {
			slog.Warn("Another connection received, closing previous one")
			q.conn.Close() // Close previous connection if a new one arrives
		}
		q.conn = conn
		q.mu.Unlock()
		slog.Info("Accepted new connection", "remote_addr", conn.RemoteAddr())
		// Start reader and writer goroutines for the new connection
		q.startConnectionGoroutines(conn)
	}
}

// reconnectClient attempts to reconnect to the server (client mode) with exponential backoff.
func (q *SocketTaskQueue) reconnectClient() {
	// Exponential backoff parameters
	initialDelay := 1 * time.Second
	maxDelay := 60 * time.Second
	factor := 2.0
	jitter := 0.1 // 10% jitter

	currentDelay := initialDelay

	for {
		q.mu.Lock()
		closed := q.closed
		conn := q.conn
		q.mu.Unlock()

		if closed {
			slog.Info("Client reconnection goroutine exiting: queue is closed")
			return // Exit if the queue is closed
		}

		if conn != nil {
			// Connection is active, wait a bit before checking again
			// This sleep prevents a tight loop if the connection is active but unusable
			time.Sleep(time.Second)
			continue
		}

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

			time.Sleep(sleepDuration)

			// Increase delay, cap at maxDelay
			currentDelay = time.Duration(float64(currentDelay) * factor)
			if currentDelay > maxDelay {
				currentDelay = maxDelay
			}

			continue // Retry connection
		}

		q.mu.Lock()
		// Set the new connection as active, potentially overwriting an old handle.
		// The old handle will be closed by its associated reader/writer loops when they exit.
		q.conn = newConn
		q.mu.Unlock()

		// Let the old reader/writer loops handle closing their own connection handles via handleConnectionClose.
		// No need to explicitly close oldConn here. If oldConn exists and is different from newConn,
		// its associated goroutines should have already exited or will exit due to the original disconnection,
		// triggering handleConnectionClose(oldConn).

		slog.Info("Client reconnected successfully", "remote_addr", newConn.RemoteAddr())

		// Give the server/network a brief moment after establishing the connection
		// before starting goroutines that might immediately try to use it.
		// This might help mitigate potential race conditions where the connection
		// Start reader and writer goroutines IMMEDIATELY for the new connection
		q.startConnectionGoroutines(newConn)

		// Give loops a moment to start before signaling/proceeding.
		// This helps ensure that if the connection fails immediately,
		// the loops have a chance to run, fail, and call handleConnectionClose
		// to reset q.conn before the test or other logic might incorrectly
		// assume the connection is stable just because q.conn was briefly set.
		time.Sleep(100 * time.Millisecond) // Shorter delay after starting loops

		// Signal that a connection is potentially available (loops might fail quickly)
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

	// Cancel the context to signal goroutines to exit
	q.cancel()

	// Close the listener (if server mode)
	var err error
	if q.listener != nil {
		slog.Info("Closing listener")
		if closeErr := q.listener.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close listener: %w", closeErr)
		}
	}

	// Capture connection and listener handles under lock
	q.mu.Lock()
	connToClose := q.conn
	listenerToClose := q.listener
	q.conn = nil // Clear active connection immediately
	q.mu.Unlock() // Unlock before closing handles

	// Cancel the context first to signal goroutines
	q.cancel()

	// Close the listener (if server mode)
	var listenerErr error
	if listenerToClose != nil {
		slog.Info("Closing listener")
		if closeErr := listenerToClose.Close(); closeErr != nil {
			// Check if the error indicates the listener was already closed, potentially ignorable
			// This check might need refinement based on specific OS/net errors
			if !errors.Is(closeErr, net.ErrClosed) {
				listenerErr = fmt.Errorf("failed to close listener: %w", closeErr)
				slog.Error("Error closing listener", "error", listenerErr)
			} else {
				slog.Debug("Listener already closed, ignoring error in Close")
			}
		}
	}

	// Close the active connection handle we captured (if any)
	var connErr error
	if connToClose != nil {
		slog.Info("Closing active connection handle captured during Close", "local_addr", connToClose.LocalAddr(), "remote_addr", connToClose.RemoteAddr())
		if closeErr := connToClose.Close(); closeErr != nil {
			// Ignore "use of closed network connection" as it implies concurrent closure by handleConnectionClose
			// Also ignore net.ErrClosed for similar reasons.
			if !errors.Is(closeErr, net.ErrClosed) && !strings.Contains(closeErr.Error(), "use of closed network connection") {
				connErr = fmt.Errorf("failed to close connection handle: %w", closeErr)
				slog.Error("Error closing connection handle", "error", connErr)
			} else {
				slog.Debug("Connection handle already closed concurrently, ignoring error in Close", "local_addr", connToClose.LocalAddr(), "remote_addr", connToClose.RemoteAddr())
			}
		}
	}

	// Close the channels to signal goroutines that read from them
	close(q.writeChan)
	// Use sync.Once to ensure readChan is closed only once
	q.readChanOnce.Do(func() {
		close(q.readChan)
		slog.Debug("readChan closed by Close")
	})
	// Use sync.Once to ensure taskChan is closed only once
	q.taskChanOnce.Do(func() {
		close(q.taskChan)
		slog.Debug("taskChan closed by Close")
	})

	// Wait for all goroutines to finish
	q.wg.Wait()

	slog.Info("Socket task queue closed successfully")
	return err
}

// startConnectionGoroutines starts the reader and writer goroutines for a given connection.
func (q *SocketTaskQueue) startConnectionGoroutines(conn net.Conn) {
	q.wg.Add(2) // Add 2 to the WaitGroup for reader and writer goroutines
	go q.readerLoop(conn)
	go q.writerLoop(conn)
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
func (q *SocketTaskQueue) readerLoop(conn net.Conn) {
	defer q.wg.Done()
	defer func() {
		slog.Info("Reader loop exiting", "remote_addr", conn.RemoteAddr())
		// When reader loop exits, it means the connection is broken or closed.
		// We should close the connection and signal for reconnection (if client).
		q.handleConnectionClose(conn)
	}()

	slog.Info("Reader loop started", "remote_addr", conn.RemoteAddr())
	slog.Debug("Reader loop running for connection", "local_addr", conn.LocalAddr(), "remote_addr", conn.RemoteAddr()) // Add detailed log

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
			}
			// Handle specific recoverable errors differently
			if errors.Is(err, io.EOF) {
				slog.Info("Connection closed by remote peer during read", "remote_addr", conn.RemoteAddr())
				return // Exit loop on EOF
			}
			// Check for our custom "invalid magic number" error
			if err.Error() == "invalid magic number received" {
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
func (q *SocketTaskQueue) writerLoop(conn net.Conn) {
	defer q.wg.Done()
	defer func() {
		slog.Info("Writer loop exiting", "remote_addr", conn.RemoteAddr())
		// When writer loop exits, it means the connection is broken or closed.
		// We should close the connection and signal for reconnection (if client).
		q.handleConnectionClose(conn)
	}()

	slog.Info("Writer loop started", "remote_addr", conn.RemoteAddr())
	slog.Debug("Writer loop running for connection", "local_addr", conn.LocalAddr(), "remote_addr", conn.RemoteAddr()) // Add detailed log

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

	// Now, check if this closed connection was the active one.
	q.mu.Lock()
	isActive := (q.conn == conn) // Check if the handle we just closed *was* the active one
	if isActive {
		slog.Info("Active connection closed, clearing reference.", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr)
		q.conn = nil       // Clear the active connection reference
		q.cond.Broadcast() // Signal connection loss

		// Server-specific logic for closing taskChan on unexpected client disconnect
		if !q.closed && q.mode == "server" {
			q.taskChanOnce.Do(func() {
				close(q.taskChan)
				slog.Debug("taskChan closed by handleConnectionClose (server mode, unexpected closure)", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr)
			})
		}
	} else {
		// This means the connection handle being closed was likely from an older,
		// already replaced connection, or the active connection was already cleared
		// by the other loop (reader/writer) exiting first.
		slog.Debug("Non-active connection handle closed.", "local_addr", connLocalAddr, "remote_addr", connRemoteAddr)
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

// Define a magic number for the protocol header
var magicNumber = [4]byte{0x1A, 0x2B, 0x3C, 0x4D}

// readMessage reads a length-prefixed message with a magic number from the connection.
func (q *SocketTaskQueue) readMessage(conn net.Conn) ([]byte, error) {
	// Read the magic number
	receivedMagic := make([]byte, 4)
	if _, err := io.ReadFull(conn, receivedMagic); err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}

	// Verify the magic number
	if receivedMagic[0] != magicNumber[0] ||
		receivedMagic[1] != magicNumber[1] ||
		receivedMagic[2] != magicNumber[2] ||
		receivedMagic[3] != magicNumber[3] {
		return nil, errors.New("invalid magic number received")
	}

	// Read the length prefix (4 bytes for a uint32)
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	messageLength := uint32(lengthBytes[0]) | uint32(lengthBytes[1])<<8 | uint32(lengthBytes[2])<<16 | uint32(lengthBytes[3])<<24

	if messageLength == 0 {
		return nil, errors.New("received empty message")
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
	// Write the magic number
	if _, err := conn.Write(magicNumber[:]); err != nil {
		return fmt.Errorf("failed to write magic number: %w", err)
	}

	messageLength := uint32(len(message))
	lengthBytes := make([]byte, 4)
	lengthBytes[0] = byte(messageLength)
	lengthBytes[1] = byte(messageLength >> 8)
	lengthBytes[2] = byte(messageLength >> 16)
	lengthBytes[3] = byte(messageLength >> 24)

	// Write the length prefix
	if _, err := conn.Write(lengthBytes); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}

	// Write the message payload
	if _, err := conn.Write(message); err != nil {
		return fmt.Errorf("failed to write message payload: %w", err)
	}

	return nil
}
