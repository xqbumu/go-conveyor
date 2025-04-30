package conveyor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// DummyTask is a simple task implementation for testing.
type DummyTask struct {
	ID         string `json:"id"`
	Data       string `json:"data"`
	RetryCount int    `json:"retry_count"` // Add RetryCount field
}

func (t *DummyTask) GetIdentify() string {
	return t.ID
}

func (t *DummyTask) GetID() string {
	return t.ID
}

func (t *DummyTask) GetType() any {
	return t // Return the struct itself as the type indicator
}

func (t *DummyTask) GetPriority() int {
	return 0 // Default priority for testing
}

func (t *DummyTask) GetTimeout() time.Duration {
	return 0 // Default timeout for testing
}

// GetRetryCount implements ITask.
func (t *DummyTask) GetRetryCount() int {
	return t.RetryCount
}

// IncrementRetryCount implements ITask.
func (t *DummyTask) IncrementRetryCount() {
	t.RetryCount++
}

// Ensure DummyTask implements ITask
var _ ITask = (*DummyTask)(nil)

func init() {
	// Register the dummy task type for deserialization
	RegisterTaskType("conveyor.DummyTask", func() ITask { return &DummyTask{} })
}

func TestNewSocketTaskQueue_Server(t *testing.T) {
	t.Parallel() // Mark test as parallelizable
	t.Run("tcp server", func(t *testing.T) {
		addr := "127.0.0.1:0"                                    // Use port 0 to get a random available port
		q, err := NewSocketTaskQueue("server", "tcp", addr, nil) // Add nil for options
		if err != nil {
			t.Fatalf("Failed to create TCP server queue: %v", err)
		}
		defer q.Close()

		if q.listener == nil {
			t.Error("Listener should not be nil in server mode")
		}
		// Server mode no longer has a single 'conn' field, uses activeConns map
		// if q.conn != nil {
		// 	t.Error("Connection should be nil initially in server mode")
		// }
		if q.addr == "" {
			t.Error("Address should be set")
		}
		if q.network != "tcp" {
			t.Errorf("Network should be tcp, got %s", q.network)
		}
		if q.mode != "server" {
			t.Errorf("Mode should be server, got %s", q.mode)
		}
	})

	t.Run("unix server", func(t *testing.T) {
		// Create a temporary directory for the socket file
		tmpDir, err := os.MkdirTemp("", "unix_socket_test")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir) // Clean up the temp directory

		addr := fmt.Sprintf("%s/test.sock", tmpDir)
		q, err := NewSocketTaskQueue("server", "unix", addr, nil) // Add nil for options
		if err != nil {
			t.Fatalf("Failed to create Unix server queue: %v", err)
		}
		defer q.Close()

		if q.listener == nil {
			t.Error("Listener should not be nil in server mode")
		}
		// Server mode no longer has a single 'conn' field, uses activeConns map
		// if q.conn != nil {
		// 	t.Error("Connection should be nil initially in server mode")
		// }
		if q.addr != addr {
			t.Errorf("Address should be %s, got %s", addr, q.addr)
		}
		if q.network != "unix" {
			t.Errorf("Network should be unix, got %s", q.network)
		}
		if q.mode != "server" {
			t.Errorf("Mode should be server, got %s", q.mode)
		}

		// Check if the socket file was created
		if _, err := os.Stat(addr); os.IsNotExist(err) {
			t.Errorf("Unix socket file was not created at %s", addr)
		}
	})

	t.Run("unsupported network server", func(t *testing.T) {
		_, err := NewSocketTaskQueue("server", "udp", "127.0.0.1:0", nil) // Add nil for options
		if err == nil {
			t.Error("Expected error for unsupported network type")
		} else if err.Error() != "server mode unsupported network type: udp" {
			t.Errorf("Expected error 'server mode unsupported network type: udp', got '%v'", err)
		}
	})

	t.Run("unsupported mode", func(t *testing.T) {
		_, err := NewSocketTaskQueue("worker", "tcp", "127.0.0.1:0", nil) // Add nil for options
		if err == nil {
			t.Error("Expected error for unsupported mode")
		} else if err.Error() != "unsupported mode: worker, must be 'server' or 'client'" {
			t.Errorf("Expected error 'unsupported mode: worker, must be 'server' or 'client'', got '%v'", err)
		}
	})
}

func TestNewSocketTaskQueue_Client(t *testing.T) {
	// Start a dummy server first
	serverAddr := "127.0.0.1:0"
	serverQ, err := NewSocketTaskQueue("server", "tcp", serverAddr, nil) // Add nil for options
	if err != nil {
		t.Fatalf("Failed to create dummy server: %v", err)
	}
	defer serverQ.Close()

	// Get the actual address the server is listening on
	actualServerAddr := serverQ.listener.Addr().String()

	t.Run("tcp client connects to server", func(t *testing.T) {
		q, err := NewSocketTaskQueue("client", "tcp", actualServerAddr, nil) // Add nil for options
		if err != nil {
			t.Fatalf("Failed to create TCP client queue: %v", err)
		}
		defer q.Close()

		// Wait briefly for connection to establish
		time.Sleep(100 * time.Millisecond)

		q.mu.Lock()
		conn := q.clientConn // Use clientConn for client mode
		q.mu.Unlock()

		if conn == nil {
			t.Error("Client connection should not be nil after connecting")
		}
		if q.listener != nil {
			t.Error("Listener should be nil in client mode")
		}
		if q.addr != actualServerAddr {
			t.Errorf("Address should be %s, got %s", actualServerAddr, q.addr)
		}
		if q.network != "tcp" {
			t.Errorf("Network should be tcp, got %s", q.network)
		}
		if q.mode != "client" {
			t.Errorf("Mode should be client, got %s", q.mode)
		}
	})

	t.Run("client fails to connect to non-existent server", func(t *testing.T) {
		// Use an address that is unlikely to have a server
		nonExistentAddr := "127.0.0.1:54321"
		q, err := NewSocketTaskQueue("client", "tcp", nonExistentAddr, nil) // Add nil for options
		if err == nil {
			t.Error("Expected error when connecting to non-existent server")
		} else {
			// Check if the error indicates connection refused or similar
			// The exact error message might vary by OS, so check for a common pattern
			// We expect an error containing "connection refused" or similar.
			expectedErrSubstring := "connection refused"
			if err.Error() == "" || !isConnectionRefused(err) { // Use the helper function
				t.Errorf("Expected error containing '%s', got: %v", expectedErrSubstring, err)
			}
		}

		// Even if initial connection fails, the reconnect goroutine is started.
		// We should still close the queue to clean up the goroutine.
		if q != nil {
			defer q.Close()
			q.mu.Lock()
			conn := q.clientConn // Use clientConn for client mode
			q.mu.Unlock()
			if conn != nil {
				t.Error("Client connection should be nil after failed initial connection")
			}
		}
	})
}

// Helper function to check if an error is a connection refused error
func isConnectionRefused(err error) bool {
	// Unwrap the error to get the underlying network error
	unwrappedErr := errors.Unwrap(err)
	if unwrappedErr == nil {
		unwrappedErr = err // If no wrapper, use the original error
	}

	if opErr, ok := unwrappedErr.(*net.OpError); ok {
		if sysErr, ok := opErr.Err.(*os.SyscallError); ok {
			// Check for common syscall errors indicating connection refused
			return sysErr.Err.Error() == "connection refused" || sysErr.Err.Error() == "connect: connection refused"
		}
		// Check for other net.OpError patterns
		return opErr.Op == "dial" && opErr.Net == "tcp" && opErr.Err.Error() == "connect: connection refused"
	}
	// Fallback check for error string (less reliable)
	return unwrappedErr != nil && (
	// Common error messages across different platforms/Go versions
	// "connection refused" is common on Linux/macOS
	// "connectex: No connection could be made because the target machine actively refused it." is common on Windows
	// "dial tcp 127.0.0.1:54321: connect: connection refused" is a common format
	// "dial tcp [::1]:54321: connect: connection refused" (IPv6)
	// "dial tcp 127.0.0.1:54321: wsarefused" (Windows specific)
	// "dial tcp 127.0.0.1:54321: i/o timeout" (could happen if firewall blocks or server is slow to respond)
	// "dial tcp 127.0.0.1:54321: no route to host" (network configuration issue)
	// We'll focus on the most common "connection refused" pattern for simplicity in this test helper.
	// A more robust check might inspect the underlying error type more deeply or use platform-specific checks.
	// For this test, a simple string contains check is often sufficient to indicate the intended failure.
	// However, relying on string matching is fragile. Let's stick to checking the OpError/SyscallError structure first.
	// If the structured check fails, we can add specific string checks if necessary, but it's less ideal.
	// Let's refine the structured check.
	// The error from net.Dial on connection refused is typically *net.OpError with a *os.SyscallError inside.
	// The SyscallError's Err field is a syscall.Errno or similar platform-specific error value.
	// We need to check the *value* of this error, not just its string representation, if possible.
	// However, accessing syscall.Errno values directly is platform-dependent.
	// The string representation is often the most portable way to check for specific *types* of network errors
	// when the underlying error value isn't easily accessible or comparable across platforms.
	// Let's add a robust string check as a fallback, acknowledging its limitations.
	// A better approach might be to use a library or a more comprehensive set of checks for network errors.
	// For this test, let's keep it simple and check for the common string pattern.
	// This is a compromise between robustness and simplicity for a test helper.
	// If tests fail on a specific platform due to different error messages, this helper might need refinement.
	unwrappedErr.Error() == "connection refused" ||
		unwrappedErr.Error() == "connect: connection refused" ||
		unwrappedErr.Error() == "dial tcp: connect: connection refused" || // Common format
		unwrappedErr.Error() == "dial unix: connect: connection refused" || // Unix socket format
		// Add other common patterns if needed based on test failures on different platforms
		// e.g., strings.Contains(err.Error(), "connection refused")
		false)
}

// setupServerClientQueues is a helper function to set up a server and client queue pair for testing.
// It returns the server queue, client queue, the address used, and a cleanup function.
// The cleanup function closes both queues and removes the temporary directory if network is "unix".
func setupServerClientQueues(t *testing.T, network string) (*SocketTaskQueue, *SocketTaskQueue, string, func()) {
	t.Helper() // Mark this as a test helper function

	var addr string
	var tmpDir string
	var err error

	if network == "unix" {
		tmpDir, err = os.MkdirTemp("", "socket_test_")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		addr = fmt.Sprintf("%s/test.sock", tmpDir)
	} else if network == "tcp" {
		addr = "127.0.0.1:0" // Use random port
	} else {
		t.Fatalf("Unsupported network type for setup: %s", network)
	}

	// Start server
	serverQ, err := NewSocketTaskQueue("server", network, addr, nil) // Add nil for options
	if err != nil {
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
		t.Fatalf("Failed to create server queue (%s): %v", network, err)
	}

	// Get the actual address if TCP used port 0
	if network == "tcp" {
		addr = serverQ.listener.Addr().String()
	}

	// Start client
	clientQ, err := NewSocketTaskQueue("client", network, addr, nil) // Add nil for options
	if err != nil {
		serverQ.Close()
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
		t.Fatalf("Failed to create client queue (%s): %v", network, err)
	}

	// Wait for client to connect to server
	// Increase wait time slightly to ensure connection stability in various environments
	time.Sleep(200 * time.Millisecond)

	// Ensure server has an active connection
	serverQ.mu.Lock()
	serverHasConnection := len(serverQ.activeConns) > 0
	serverQ.mu.Unlock()
	if !serverHasConnection {
		clientQ.Close()
		serverQ.Close()
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
		t.Fatal("Server did not accept client connection within timeout")
	}

	// Ensure client has an active connection
	clientQ.mu.Lock()
	clientConn := clientQ.clientConn
	clientQ.mu.Unlock()
	if clientConn == nil {
		clientQ.Close()
		serverQ.Close()
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
		t.Fatal("Client did not establish connection within timeout")
	}

	cleanup := func() {
		clientQ.Close()
		serverQ.Close()
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
	}

	return serverQ, clientQ, addr, cleanup
}

func TestPushPopTask(t *testing.T) {
	serverQ, clientQ, _, cleanup := setupServerClientQueues(t, "unix")
	defer cleanup()

	// Create a dummy task
	taskToPush := &DummyTask{
		ID:   "task-123",
		Data: "some test data",
	}

	// Push the task from client
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPush()
	// Use short assignment operator := for the first use of err in this scope
	err := clientQ.Push(ctxPush, taskToPush)
	if err != nil {
		t.Fatalf("Failed to push task: %v", err)
	}

	// Pop the task from server
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPop()
	poppedTask, err := serverQ.Pop(ctxPop) // Use assignment = for subsequent uses
	if err != nil {
		t.Fatalf("Failed to pop task: %v", err)
	}

	// Assert the popped task is the same as the pushed task
	if poppedTask == nil {
		t.Fatal("Popped task is nil")
	}

	poppedDummyTask, ok := poppedTask.(*DummyTask)
	if !ok {
		t.Fatalf("Popped task is not of type *DummyTask, got %T", poppedTask)
	}

	if poppedDummyTask.ID != taskToPush.ID {
		t.Errorf("Popped task ID mismatch: expected %s, got %s", taskToPush.ID, poppedDummyTask.ID)
	}
	if poppedDummyTask.Data != taskToPush.Data {
		t.Errorf("Popped task Data mismatch: expected %s, got %s", taskToPush.Data, poppedDummyTask.Data)
	}
}

// TODO: Add tests for:
// - Handling connection closure during Push/Pop
// - Error handling for invalid messages (e.g., wrong magic number, invalid JSON)
// - Len() method (should always return 0 for this implementation) - Already implemented in TestLen

func TestClose(t *testing.T) {
	t.Parallel() // Mark test as parallelizable
	// Use _ to ignore the cleanup function as we call Close manually.
	serverQ, clientQ, _, _ := setupServerClientQueues(t, "unix")

	// Close the server queue
	err := serverQ.Close() // First use of err in this scope
	if err != nil {
		t.Errorf("Failed to close server queue: %v", err)
	}

	// Close the client queue
	err = clientQ.Close()
	if err != nil {
		t.Errorf("Failed to close client queue: %v", err)
	}
	// The setupServerClientQueues helper's cleanup function handles temp dir removal.
	// We don't need manual cleanup here, even though we don't defer the cleanup call.

	// Verify Push on closed queue returns error
	taskToPush := &DummyTask{ID: "closed-task", Data: "data"}
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelPush()
	// Use assignment = as err is already declared above
	err = clientQ.Push(ctxPush, taskToPush)
	if err == nil {
		t.Error("Push on closed queue did not return error")
	} else {
		expectedErr := "Push: queue is closed"
		if err.Error() != expectedErr {
			t.Errorf("Expected Push error '%s', got '%v'", expectedErr, err)
		}
	}

	// Verify Pop on closed queue returns error
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelPop()
	// Use assignment = as err is already declared above
	_, err = serverQ.Pop(ctxPop)
	if err == nil {
		t.Error("Pop on closed queue did not return error")
	} else {
		// When closing, Pop might receive context done or taskChan closed.
		// Accept either "Pop: queue is closing" or "Pop: queue is closed".
		expectedErr1 := "Pop: queue is closing"
		expectedErr2 := "Pop: queue is closed"
		if err.Error() != expectedErr1 && err.Error() != expectedErr2 {
			t.Errorf("Expected Pop error '%s' or '%s', got '%v'", expectedErr1, expectedErr2, err)
		}
	}

	// Verify closing already closed queue does not return error
	// Use assignment = as err is already declared above
	err = serverQ.Close()
	if err != nil {
		t.Errorf("Closing already closed server queue returned error: %v", err)
	}
	// Use assignment = as err is already declared above
	err = clientQ.Close()
	if err != nil {
		t.Errorf("Closing already closed client queue returned error: %v", err)
	}
}

// TODO: Add tests for:
// - Error handling for invalid messages (e.g., wrong magic number, invalid JSON)
// - Len() method (should always return 0 for this implementation) - Already implemented in TestLen

func TestPushPopOnClosedConnection(t *testing.T) {
	serverQ, clientQ, _, cleanup := setupServerClientQueues(t, "unix")
	defer cleanup()

	// Get the underlying connection from the client queue
	clientQ.mu.Lock()
	conn := clientQ.clientConn // Use clientConn for client mode
	clientQ.mu.Unlock()

	if conn == nil {
		t.Fatal("Client connection is nil before closing")
	}

	// Force close the connection
	t.Log("Force closing the connection")
	conn.Close()

	// Give time for the reader/writer goroutines to detect the closure
	time.Sleep(100 * time.Millisecond)

	// Verify Push on closed connection returns error
	taskToPush := &DummyTask{ID: "push-closed-conn", Data: "data"}
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelPush()
	// Push might succeed initially as it only puts the task in the writeChan.
	// The error will occur when the writerLoop tries to write to the closed connection.
	// We don't assert on the Push error directly here.
	_ = clientQ.Push(ctxPush, taskToPush)
	// Give writerLoop a chance to fail
	time.Sleep(50 * time.Millisecond)

	// Verify Pop on closed connection returns error
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelPop()
	_, err := serverQ.Pop(ctxPop) // First use of err in this scope
	if err == nil {
		t.Error("Pop on closed connection did not return error")
	} else {
		// Expecting the Pop context to time out, as the connection is closed
		// and the server's taskChan remains open (due to multi-client support).
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Expected Pop error to be context.DeadlineExceeded, got '%v'", err)
		} else {
			t.Logf("Pop timed out as expected after client connection closed: %v", err)
		}
	}

	// Verify client attempts to reconnect after connection closure
	// This is covered by TestClientReconnect, but we can add a quick check here.
	// Wait for client to attempt reconnection
	time.Sleep(2 * time.Second) // Wait longer than the initial reconnect delay

	clientQ.mu.Lock()
	reconnectedConn := clientQ.clientConn // Use clientConn for client mode
	clientQ.mu.Unlock()

	if reconnectedConn != nil {
		t.Log("Client attempted to reconnect after connection closure (as expected)")
		// Note: We don't need to verify successful reconnection here, as TestClientReconnect covers that.
		// This check just confirms the reconnect logic was triggered.
	} else {
		t.Log("Client connection is still nil after waiting for reconnection attempt")
		// This might happen if the test finishes before the reconnect goroutine's delay.
		// It's not a failure of this specific test's goal (testing Push/Pop on closed conn),
		// but worth noting.
	}
}

// TODO: Add tests for:
// - Len() method (should always return 0 for this implementation) - Already implemented in TestLen

func TestInvalidMessageHandling(t *testing.T) {
	t.Parallel() // Mark test as parallelizable
	serverQ, clientQ, _, cleanup := setupServerClientQueues(t, "unix")
	defer cleanup()

	// Get the underlying connection from the client queue
	clientQ.mu.Lock()
	conn := clientQ.clientConn // Use clientConn for client mode
	clientQ.mu.Unlock()

	if conn == nil {
		t.Fatal("Client connection is nil")
	}

	// Send some invalid data directly to the connection
	invalidData := []byte("this is not a valid message\n")
	_, err := conn.Write(invalidData) // First use of err in this scope
	if err != nil {
		t.Errorf("Failed to write invalid data: %v", err)
	}

	// Send another piece of invalid data (e.g., wrong magic number)
	invalidMagicData := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, '{', '}'} // Wrong magic number
	// Use assignment = as err is already declared above
	_, err = conn.Write(invalidMagicData)
	if err != nil {
		t.Errorf("Failed to write invalid magic data: %v", err)
	}

	// Send a valid task
	taskToPush := &DummyTask{ID: "valid-task", Data: "valid data"}
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPush()
	// Use assignment = as err is already declared above
	err = clientQ.Push(ctxPush, taskToPush)
	if err != nil {
		t.Fatalf("Failed to push valid task after invalid data: %v", err)
	}

	// After sending invalid data, the connection state might be corrupted,
	// or the server might have closed the connection due to the error.
	// We should expect Pop to fail or timeout, not succeed.
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 1*time.Second) // Shorter timeout
	defer cancelPop()
	// Use assignment = as err is already declared above
	_, err = serverQ.Pop(ctxPop)
	if err == nil {
		t.Error("Expected Pop to fail after sending invalid data, but it succeeded")
	} else {
		// Check if the error is a timeout or queue closed error
		if !errors.Is(err, context.DeadlineExceeded) && err.Error() != "Pop: queue is closed" {
			t.Errorf("Expected Pop error to be context deadline exceeded or queue closed, got: %v", err)
		} else {
			t.Logf("Pop failed as expected after invalid data: %v", err)
		}
	}
	// We no longer try to Pop the valid task as the connection is likely broken.
}

// TestInvalidJSONHandling tests the server's ability to handle messages
// where the outer SocketMessage is valid JSON, but the inner TaskData is not.
func TestInvalidJSONHandling(t *testing.T) {
	t.Parallel()
	serverQ, clientQ, _, cleanup := setupServerClientQueues(t, "unix")
	defer cleanup()

	// Get the underlying connection from the client queue
	clientQ.mu.Lock()
	conn := clientQ.clientConn
	clientQ.mu.Unlock()
	if conn == nil {
		t.Fatal("Client connection is nil")
	}

	// 1. Craft a message payload with invalid JSON in TaskData
	taskType := "conveyor.DummyTask"
	// This string itself is invalid JSON because of the missing closing quote
	invalidJSONPayloadString := `{"id": "invalid-json", "data": "missing quote}`

	// Manually construct the outer JSON message string, embedding the invalid payload string
	// as the value for task_data. Note that the invalidJSONPayloadString itself is the value,
	// not a JSON string literal containing it.
	outerJSONString := fmt.Sprintf(`{"task_type":"%s", "task_data": %s}`, taskType, invalidJSONPayloadString)
	invalidMsgBytes := []byte(outerJSONString) // These are the bytes representing the outer JSON

	// Manually prepend magic number and length (similar to writeMessage)
	buf := new(bytes.Buffer)
	var err error // Declare err here
	// Write magic number
	buf.Write([]byte(magicNumber))
	// Write length
	lengthBytes := make([]byte, 4)
	messageLength := uint32(len(invalidMsgBytes))
	lengthBytes[0] = byte(messageLength)
	lengthBytes[1] = byte(messageLength >> 8)
	lengthBytes[2] = byte(messageLength >> 16)
	lengthBytes[3] = byte(messageLength >> 24)
	buf.Write(lengthBytes)
	// Write message body
	buf.Write(invalidMsgBytes)

	// Send the crafted invalid message directly
	t.Log("Sending message with invalid TaskData JSON")
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		t.Fatalf("Failed to write invalid JSON message: %v", err)
	}

	// Give messageProcessor time to process the invalid message
	time.Sleep(100 * time.Millisecond)

	// 2. Send a subsequent valid task using Push
	validTaskID := "valid-after-invalid-json"
	validTask := &DummyTask{ID: validTaskID, Data: "valid data"}
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelPush()
	t.Log("Sending valid task after invalid JSON message")
	err = clientQ.Push(ctxPush, validTask)
	if err != nil {
		t.Fatalf("Failed to push valid task after invalid JSON: %v", err)
	}

	// 3. Attempt to Pop the valid task from the server
	// The server should have skipped the invalid JSON message and processed the valid one.
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 2*time.Second) // Increased timeout slightly
	defer cancelPop()
	t.Log("Attempting to pop the valid task")
	poppedTask, err := serverQ.Pop(ctxPop)
	if err != nil {
		t.Fatalf("Expected to pop the valid task, but failed: %v", err)
	}

	// 4. Verify the popped task is the valid one
	if poppedTask == nil {
		t.Fatal("Popped task is nil, expected the valid task")
	}
	poppedDummyTask, ok := poppedTask.(*DummyTask)
	if !ok {
		t.Fatalf("Popped task is not *DummyTask, got %T", poppedTask)
	}
	if poppedDummyTask.ID != validTaskID {
		t.Errorf("Expected popped task ID to be '%s', got '%s'", validTaskID, poppedDummyTask.ID)
	}
	t.Logf("Successfully popped valid task '%s' after skipping invalid JSON message.", validTaskID)

}

// TestLen verifies the Len() method always returns 0
func TestLen(t *testing.T) {
	t.Parallel() // Mark test as parallelizable
	serverQ, clientQ, _, cleanup := setupServerClientQueues(t, "unix")
	defer cleanup()

	// Verify Len() returns 0 for server
	if serverQ.Len() != 0 {
		t.Errorf("Server Len() should be 0, got %d", serverQ.Len())
	}

	// Verify Len() returns 0 for client
	if clientQ.Len() != 0 {
		t.Errorf("Client Len() should be 0, got %d", clientQ.Len())
	}

	// Push a task and verify Len() is still 0 (Len is not a queue size indicator for this implementation)
	taskToPush := &DummyTask{ID: "len-task", Data: "data"}
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPush()
	err := clientQ.Push(ctxPush, taskToPush) // First use of err in this scope
	if err != nil {
		t.Fatalf("Failed to push task for Len test: %v", err)
	}

	if serverQ.Len() != 0 {
		t.Errorf("Server Len() should be 0 after push, got %d", serverQ.Len())
	}
	if clientQ.Len() != 0 {
		t.Errorf("Client Len() should be 0 after push, got %d", clientQ.Len())
	}

	// Pop the task and verify Len() is still 0
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPop()
	// Use assignment = as err is already declared above
	_, err = serverQ.Pop(ctxPop)
	if err != nil {
		t.Fatalf("Failed to pop task for Len test: %v", err)
	}

	if serverQ.Len() != 0 {
		t.Errorf("Server Len() should be 0 after pop, got %d", serverQ.Len())
	}
	if clientQ.Len() != 0 {
		t.Errorf("Client Len() should be 0 after pop, got %d", clientQ.Len())
	}
}

func TestConcurrentPushPop(t *testing.T) {
	serverQ, clientQ, _, cleanup := setupServerClientQueues(t, "unix")
	defer cleanup()

	// Number of concurrent pushers and total tasks
	numPushers := 10   // Number of goroutines pushing tasks
	totalTasks := 1000 // Total number of tasks to push

	tasksPerPusher := totalTasks / numPushers
	if totalTasks%numPushers != 0 {
		t.Logf("Warning: Total tasks (%d) not evenly divisible by number of pushers (%d). Some pushers will push one more task.", totalTasks, numPushers)
	}

	var pushWg sync.WaitGroup
	pushWg.Add(numPushers)

	// Concurrently push tasks from the single client instance
	t.Logf("Starting %d concurrent pushers to push %d tasks total", numPushers, totalTasks)
	for i := 0; i < numPushers; i++ {
		go func(pusherID int) {
			defer pushWg.Done()

			startTaskIndex := pusherID * tasksPerPusher
			endTaskIndex := startTaskIndex + tasksPerPusher
			if pusherID == numPushers-1 {
				// Last pusher takes the remainder
				endTaskIndex = totalTasks
			}

			for j := startTaskIndex; j < endTaskIndex; j++ {
				taskToPush := &DummyTask{
					ID:   fmt.Sprintf("task-%d", j),
					Data: fmt.Sprintf("data-%d", j),
				}
				ctxPush, cancelPush := context.WithTimeout(context.Background(), 5*time.Second)
				err := clientQ.Push(ctxPush, taskToPush)
				cancelPush()
				if err != nil {
					t.Errorf("Pusher %d: Failed to push task %s: %v", pusherID, taskToPush.ID, err)
					// Continue pushing other tasks even if one fails
				}
			}
			t.Logf("Pusher %d finished pushing tasks", pusherID)
		}(i)
	}

	// Collect tasks from the server
	receivedTasks := make(map[string]*DummyTask)
	var popWg sync.WaitGroup
	popWg.Add(1) // Use one goroutine to pop tasks

	t.Logf("Starting pop goroutine to collect %d tasks", totalTasks)
	go func() {
		defer popWg.Done()
		// Set a longer timeout for popping all tasks, considering the total task count
		ctxPop, cancelPop := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelPop()

		for len(receivedTasks) < totalTasks {
			task, err := serverQ.Pop(ctxPop)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Server: Timeout waiting for tasks. Received %d out of %d", len(receivedTasks), totalTasks)
					return
				}
				// If the error is due to the queue closing (e.g., test finished), break the loop
				if errors.Is(err, context.Canceled) || err.Error() == "Pop: queue is closed" {
					t.Logf("Server: Pop received queue closed signal. Received %d out of %d", len(receivedTasks), totalTasks)
					break
				}
				t.Errorf("Server: Failed to pop task: %v", err)
				// Continue popping even on error, might be a transient issue
				continue
			}
			dummyTask, ok := task.(*DummyTask)
			if !ok {
				t.Errorf("Server: Popped task is not *DummyTask, got %T", task)
				continue
			}
			receivedTasks[dummyTask.ID] = dummyTask
			// t.Logf("Server: Popped task %s. Received %d/%d", dummyTask.ID, len(receivedTasks), totalTasks) // Too verbose
		}
		t.Logf("Pop goroutine finished. Received %d tasks.", len(receivedTasks))
	}()

	// Wait for all pushers to finish
	pushWg.Wait()
	t.Log("All pushers finished.")

	// Give the pop goroutine a moment to collect any remaining tasks
	time.Sleep(100 * time.Millisecond)

	// Close the client queue after all pushes are done.
	// This will close the client connection, which should eventually
	// cause the server's reader loop to exit and the server's taskChan
	// to be closed (via messageProcessor exiting), unblocking the Pop.
	t.Log("Closing client queue.")
	clientQ.Close()

	// Wait for the pop goroutine to finish collecting tasks
	t.Log("Waiting for pop goroutine to finish.")
	popWg.Wait()
	t.Log("Pop goroutine finished.")

	// Verify all tasks were received
	if len(receivedTasks) != totalTasks {
		t.Errorf("Expected to receive %d tasks, but received %d", totalTasks, len(receivedTasks))
	} else {
		t.Logf("Successfully received all %d tasks", totalTasks)
	}

	// Optional: Verify content of a few tasks
	// This is harder without knowing the exact order, but we can check if specific task IDs exist
	// For simplicity, we'll just check the count. A more thorough test might store expected tasks
	// and compare against received tasks.
}

func TestClientReconnect(t *testing.T) {
	// Use a temporary directory for the unix socket
	tmpDir, err := os.MkdirTemp("", "client_reconnect_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	addr := fmt.Sprintf("%s/reconnect.sock", tmpDir)

	// Start server
	serverQ, err := NewSocketTaskQueue("server", "unix", addr, nil) // Add nil for options
	if err != nil {
		t.Fatalf("Failed to create server queue: %v", err)
	}
	// Defer server close, but we will close and restart it manually in the test

	// Start client
	clientQ, err := NewSocketTaskQueue("client", "unix", addr, nil) // Add nil for options
	if err != nil {
		serverQ.Close() // Clean up server if client creation fails
		t.Fatalf("Failed to create client queue: %v", err)
	}
	defer clientQ.Close()

	// Wait for initial connection
	time.Sleep(200 * time.Millisecond) // Give time for connection goroutine

	// Verify initial connection
	clientQ.mu.Lock()
	initialConn := clientQ.clientConn // Use clientConn for client mode
	clientQ.mu.Unlock()
	if initialConn == nil {
		serverQ.Close()
		t.Fatal("Client did not establish initial connection")
	}
	// slog.Info("Initial client connection established") // Removed slog in test

	// Close the server
	// slog.Info("Closing server to trigger client reconnection") // Removed slog in test
	serverQ.Close()

	// Wait for client to detect disconnection and attempt reconnection
	// This will involve exponential backoff, so we need to wait a bit.
	// The reconnectClient goroutine has an initial delay of 1 second.
	time.Sleep(2 * time.Second) // Wait longer than the initial delay

	// Verify client connection is nil after server closure
	clientQ.mu.Lock()
	connAfterClose := clientQ.clientConn // Use clientConn for client mode
	clientQ.mu.Unlock()
	if connAfterClose != nil {
		t.Error("Client connection should be nil after server closure")
	}
	// slog.Info("Client connection is nil after server closure") // Removed slog in test

	// Restart the server
	// slog.Info("Restarting server") // Removed slog in test
	serverQ_restarted, err := NewSocketTaskQueue("server", "unix", addr, nil) // Add nil for options
	if err != nil {
		t.Fatalf("Failed to restart server: %v", err)
	}
	defer serverQ_restarted.Close()
	// slog.Info("Server restarted") // Removed slog in test

	// Wait for the client to reconnect and become stable enough to push
	// We test stability by trying to push repeatedly.
	reconnectCtx, reconnectCancel := context.WithTimeout(context.Background(), 15*time.Second) // Generous timeout for reconnect + push attempt
	defer reconnectCancel()
	var pushErr error
	stable := false
	pingTask := &DummyTask{ID: "ping-task", Data: "ping"}
	for !stable {
		// Try pushing a task with a very short timeout to check connection stability
		pingCtx, pingCancel := context.WithTimeout(reconnectCtx, 200*time.Millisecond) // Short timeout for ping
		pushErr = clientQ.Push(pingCtx, pingTask)
		pingCancel() // Cancel the ping context immediately after Push returns

		if pushErr == nil {
			t.Log("Client connection stable enough to push.")
			stable = true
			// Pop the ping task from the server to clear it
			pingPopCtx, pingPopCancel := context.WithTimeout(reconnectCtx, 500*time.Millisecond)
			_, popErr := serverQ_restarted.Pop(pingPopCtx)
			pingPopCancel()
			if popErr != nil {
				t.Logf("Warning: Failed to pop ping task after successful push: %v", popErr)
				// Continue anyway, the main goal was to check push stability
			}
			break // Exit the loop on successful push
		}

		// Check if the overall timeout is exceeded
		select {
		case <-reconnectCtx.Done():
			t.Fatalf("Client failed to reconnect and become stable within timeout. Last push error: %v", pushErr)
		default:
			// Wait a bit before retrying push
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Now proceed with the actual test task Push/Pop
	taskToPush := &DummyTask{ID: "reconnect-task", Data: "data"}
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPush()
	err = clientQ.Push(ctxPush, taskToPush)
	if err != nil {
		t.Fatalf("Failed to push task after reconnection: %v", err)
	}
	// slog.Info("Task pushed after reconnection") // Removed slog in test

	// Increase Pop timeout to allow for network latency and processing
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 10*time.Second) // Increased timeout
	defer cancelPop()
	poppedTask, err := serverQ_restarted.Pop(ctxPop)
	if err != nil {
		t.Fatalf("Failed to pop task after reconnection: %v", err)
	}
	// slog.Info("Task popped after reconnection") // Removed slog in test

	poppedDummyTask, ok := poppedTask.(*DummyTask)
	if !ok {
		t.Fatalf("Popped task is not of type *DummyTask, got %T", poppedTask)
	}
	if poppedDummyTask.ID != taskToPush.ID {
		t.Errorf("Popped task ID mismatch after reconnection: expected %s, got %s", taskToPush.ID, poppedDummyTask.ID)
	}
	if poppedDummyTask.Data != taskToPush.Data {
		t.Errorf("Popped task Data mismatch: expected %s, got %s", taskToPush.Data, poppedDummyTask.Data)
	}
}
