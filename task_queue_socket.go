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
}

// NewSocketTaskQueue creates a new SocketTaskQueue.
// mode should be "server" or "client".
// network should be "tcp" or "unix".
// addr is the address to listen on (server) or connect to (client).
func NewSocketTaskQueue(mode, network, addr string) (*SocketTaskQueue, error) {
	q := &SocketTaskQueue{
		addr:    addr,
		network: network,
		mode:    mode,
	}

	var err error
	switch mode {
	case "server":
		if network == "tcp" {
			q.listener, err = net.Listen("tcp", addr)
			if err != nil {
				return nil, fmt.Errorf("failed to listen on tcp %s: %w", addr, err)
			}
			slog.Info("Server listening on tcp", "addr", addr)
			go q.acceptConnections() // Start accepting connections
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
			go q.acceptConnections() // Start accepting connections
		} else {
			return nil, fmt.Errorf("server mode unsupported network type: %s", network)
		}
	case "client":
		slog.Info("Client connecting to", "network", network, "addr", addr)
		// Seed random for jitter in reconnection
		rand.Seed(time.Now().UnixNano())

		// Attempt initial connection
		q.conn, err = net.Dial(network, addr)
		if err != nil {
			slog.Error("Initial client connection failed", "error", err)
			// Start reconnect goroutine even if initial connection fails
			go q.reconnectClient()
			// Return error to the caller if initial connection fails
			return nil, fmt.Errorf("initial client connection failed: %w", err)
		} else {
			slog.Info("Client connected successfully", "remote_addr", q.conn.RemoteAddr())
			// Signal that a connection is available
			q.cond.Broadcast()
		}

		// Always start client reconnection logic in a goroutine
		go q.reconnectClient()

		// Return the queue instance on successful initial connection
		return q, nil
	default:
		return nil, fmt.Errorf("unsupported mode: %s, must be 'server' or 'client'", mode)
	}

	// Initialize condition variable for both modes
	q.cond = sync.NewCond(&q.mu)
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
		// Handle the connection in a goroutine if needed for concurrent operations,
		// but for a simple queue, one connection might be sufficient.
		// For now, we assume one active connection at a time.
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
		q.conn = newConn
		q.mu.Unlock()
		slog.Info("Client reconnected successfully", "remote_addr", newConn.RemoteAddr())

		// Signal that a connection is available
		q.cond.Broadcast()

		// Reset delay on successful connection
		currentDelay = initialDelay
	}
}

// Push adds a task to the queue by sending it over the socket.
// It waits for a connection to be available, respecting the context deadline.
func (q *SocketTaskQueue) Push(ctx context.Context, task ITask) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Wait for a connection if none is available
	for q.conn == nil && !q.closed {
		slog.Debug("Push: Waiting for connection")
		// Wait for signal or context cancellation
		select {
		case <-ctx.Done():
			slog.Debug("Push: Context done while waiting for connection", "error", ctx.Err())
			return fmt.Errorf("Push: context done while waiting for connection: %w", ctx.Err())
		default:
			// Wait on the condition variable. This releases the mutex and re-acquires it when signaled.
			q.cond.Wait()
		}
	}

	if q.closed {
		return errors.New("Push: queue is closed")
	}

	conn := q.conn // Use the established connection

	// Marshal the task data into json.RawMessage
	taskDataBytes, err := json.Marshal(task)
	if err != nil {
		slog.Error("Failed to marshal task data", "identifier", task.GetIdentify(), "error", err)
		return fmt.Errorf("Push: failed to marshal task data for task %s: %w", task.GetIdentify(), err)
	}

	// Create a SocketMessage
	message := SocketMessage{
		TaskType: fmt.Sprintf("%T", task.GetType()), // Use fmt.Sprintf("%T", ...) to get the type name
		TaskData: json.RawMessage(taskDataBytes),
	}

	// Marshal the SocketMessage
	messageBytes, err := json.Marshal(message)
	if err != nil {
		slog.Error("Failed to marshal socket message", "identifier", task.GetIdentify(), "error", err)
		return fmt.Errorf("Push: failed to marshal socket message for task %s: %w", task.GetIdentify(), err)
	}

	// Send the length-prefixed message
	if err := q.writeMessage(conn, messageBytes); err != nil {
		slog.Error("Failed to write message to connection", "identifier", task.GetIdentify(), "error", err)
		// Consider closing the connection on write errors if they are persistent
		return fmt.Errorf("Push: failed to write message to connection for task %s: %w", task.GetIdentify(), err)
	}

	slog.Debug("Task pushed over socket", "identifier", task.GetIdentify())
	return nil
}

// Pop retrieves a task from the queue by reading from the socket.
// It waits for a connection to be available and reads with the context deadline.
func (q *SocketTaskQueue) Pop(ctx context.Context) (ITask, error) {
	var zero ITask // Declare zero value for ITask

	q.mu.Lock()
	// Wait for a connection if none is available
	for q.conn == nil && !q.closed {
		slog.Debug("Pop: Waiting for connection")
		// Wait for signal or context cancellation
		select {
		case <-ctx.Done():
			slog.Debug("Pop: Context done while waiting for connection", "error", ctx.Err())
			q.mu.Unlock()
			return zero, fmt.Errorf("Pop: context done while waiting for connection: %w", ctx.Err())
		default:
			// Wait on the condition variable. This releases the mutex and re-acquires it when signaled.
			q.cond.Wait()
		}
	}

	if q.closed {
		q.mu.Unlock()
		return zero, errors.New("Pop: queue is closed")
	}

	conn := q.conn // Use the established connection
	q.mu.Unlock() // Release mutex before reading

	// Read the length-prefixed message
	// Use a deadline from the context for the read operation
	// Note: SetReadDeadline applies to the connection, not just this read.
	// A more robust solution might involve a background reader goroutine.
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetReadDeadline(deadline)
	} else {
		// Set a default short deadline if context has no deadline, to avoid blocking indefinitely
		// This default deadline is important for the Pop method to be non-blocking if no task is available.
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	messageBytes, err := q.readMessage(conn)
	// Clear the deadline after the read attempt
	conn.SetReadDeadline(time.Time{})

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// Timeout, no data available within the deadline
			return zero, context.DeadlineExceeded // Signal no task available within the deadline
		}
		if err == io.EOF {
			slog.Info("Connection closed by remote peer during Pop read")
			q.mu.Lock()
			q.conn = nil // Clear the connection
			q.cond.Broadcast() // Signal connection status change
			q.mu.Unlock()
			return zero, errors.New("Pop: connection closed by remote peer")
		}
		slog.Error("Failed to read message from connection during Pop", "error", err)
		// Consider closing the connection on read errors if they are persistent
		q.mu.Lock()
		q.conn = nil // Clear the connection on read error
		q.cond.Broadcast() // Signal connection status change
		q.mu.Unlock()
		return zero, fmt.Errorf("Pop: failed to read message from connection: %w", err)
	}

	// Unmarshal the SocketMessage with stricter checking
	var message SocketMessage
	decoder := json.NewDecoder(bytes.NewReader(messageBytes))
	decoder.DisallowUnknownFields() // Disallow unknown fields in the SocketMessage itself
	if err := decoder.Decode(&message); err != nil {
		slog.Error("Failed to unmarshal socket message during Pop (strict)", "error", err)
		// This indicates a malformed message or unexpected fields.
		return zero, fmt.Errorf("Pop: failed to unmarshal socket message (strict): %w", err)
	}

	// Use the message.TaskType to determine the concrete task type
	// and unmarshal message.TaskData into an instance of that type.
	taskFactory, ok := taskRegistry[message.TaskType]
	if !ok {
		slog.Error("Unknown task type received during Pop", "type", message.TaskType)
		// This indicates a protocol mismatch or missing registration.
		// Could potentially close the connection or log and skip the message.
		// Returning an error for now.
		return zero, fmt.Errorf("Pop: unknown task type: %s", message.TaskType)
	}

	task := taskFactory()
	// Unmarshal the task data into the concrete type with stricter checking
	taskDecoder := json.NewDecoder(bytes.NewReader(message.TaskData))
	taskDecoder.DisallowUnknownFields() // Disallow unknown fields in the task data
	if err := taskDecoder.Decode(task); err != nil {
		slog.Error("Failed to unmarshal task data into concrete type during Pop (strict)", "type", message.TaskType, "error", err)
		// This indicates the task data doesn't match the expected type structure or has unexpected fields.
		return zero, fmt.Errorf("Pop: failed to unmarshal task data for type %s (strict): %w", message.TaskType, err)
	}

	slog.Debug("Task popped and deserialized", "type", message.TaskType, "identifier", task.GetIdentify())
	return task, nil
}

// Len returns the current number of tasks in the queue.
// For a socket-based queue, this concept is tricky. It could represent
// the number of tasks waiting to be sent/received, but that requires
// buffering and a more complex protocol. For this simple implementation,
// we'll return 0 as we don't buffer tasks internally.
func (q *SocketTaskQueue) Len() int {
	return 0
}

// Close closes the listener and any active connection.
func (q *SocketTaskQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}
	q.closed = true
	// Signal any waiting goroutines that the queue is closed
	q.cond.Broadcast()

	slog.Info("Closing socket task queue")

	var err error
	if q.listener != nil {
		slog.Info("Closing listener")
		err = q.listener.Close()
	}

	if q.conn != nil {
		slog.Info("Closing connection")
		connErr := q.conn.Close()
		if err == nil { // Only set err if listener close didn't already set it
			err = connErr
		}
		q.conn = nil // Clear the connection on close
	}

	return err
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

// TODO: Implement client-side connection logic for a different mode of operation
// where this queue connects to a remote task queue server.
// The current implementation assumes this queue is the server listening for connections.

// TODO: Implement a proper framing protocol for sending/receiving tasks over the socket
// to handle task boundaries correctly.

// TODO: Handle different ITask implementations and their data during serialization/deserialization.
// This might involve sending type information along with the data.
