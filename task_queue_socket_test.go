package conveyor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// DummyTask is a simple task implementation for testing.
type DummyTask struct {
	ID   string `json:"id"`
	Data string `json:"data"`
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

// Ensure DummyTask implements ITask
var _ ITask = (*DummyTask)(nil)

func init() {
	// Register the dummy task type for deserialization
	RegisterTaskType("conveyor.DummyTask", func() ITask { return &DummyTask{} })
}

func TestNewSocketTaskQueue_Server(t *testing.T) {
	t.Run("tcp server", func(t *testing.T) {
		addr := "127.0.0.1:0" // Use port 0 to get a random available port
		q, err := NewSocketTaskQueue("server", "tcp", addr)
		if err != nil {
			t.Fatalf("Failed to create TCP server queue: %v", err)
		}
		defer q.Close()

		if q.listener == nil {
			t.Error("Listener should not be nil in server mode")
		}
		if q.conn != nil {
			t.Error("Connection should be nil initially in server mode")
		}
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
		q, err := NewSocketTaskQueue("server", "unix", addr)
		if err != nil {
			t.Fatalf("Failed to create Unix server queue: %v", err)
		}
		defer q.Close()

		if q.listener == nil {
			t.Error("Listener should not be nil in server mode")
		}
		if q.conn != nil {
			t.Error("Connection should be nil initially in server mode")
		}
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
		_, err := NewSocketTaskQueue("server", "udp", "127.0.0.1:0")
		if err == nil {
			t.Error("Expected error for unsupported network type")
		} else if err.Error() != "server mode unsupported network type: udp" {
			t.Errorf("Expected error 'server mode unsupported network type: udp', got '%v'", err)
		}
	})

	t.Run("unsupported mode", func(t *testing.T) {
		_, err := NewSocketTaskQueue("worker", "tcp", "127.0.0.1:0")
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
	serverQ, err := NewSocketTaskQueue("server", "tcp", serverAddr)
	if err != nil {
		t.Fatalf("Failed to create dummy server: %v", err)
	}
	defer serverQ.Close()

	// Get the actual address the server is listening on
	actualServerAddr := serverQ.listener.Addr().String()

	t.Run("tcp client connects to server", func(t *testing.T) {
		q, err := NewSocketTaskQueue("client", "tcp", actualServerAddr)
		if err != nil {
			t.Fatalf("Failed to create TCP client queue: %v", err)
		}
		defer q.Close()

		// Wait briefly for connection to establish
		time.Sleep(100 * time.Millisecond)

		q.mu.Lock()
		conn := q.conn
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
		q, err := NewSocketTaskQueue("client", "tcp", nonExistentAddr)
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
			conn := q.conn
			q.mu.Unlock()
			if conn != nil {
				t.Error("Connection should be nil after failed initial connection")
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

func TestPushPopTask(t *testing.T) {
	// Use a temporary directory for the unix socket
	tmpDir, err := os.MkdirTemp("", "push_pop_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	addr := fmt.Sprintf("%s/push_pop.sock", tmpDir)

	// Start server
	serverQ, err := NewSocketTaskQueue("server", "unix", addr)
	if err != nil {
		t.Fatalf("Failed to create server queue: %v", err)
	}
	defer serverQ.Close()

	// Start client
	clientQ, err := NewSocketTaskQueue("client", "unix", addr)
	if err != nil {
		t.Fatalf("Failed to create client queue: %v", err)
	}
	defer clientQ.Close()

	// Wait for client to connect to server
	time.Sleep(100 * time.Millisecond) // Give time for connection goroutine

	// Ensure server has an active connection
	serverQ.mu.Lock()
	serverConn := serverQ.conn
	serverQ.mu.Unlock()
	if serverConn == nil {
		t.Fatal("Server did not accept client connection")
	}

	// Ensure client has an active connection
	clientQ.mu.Lock()
	clientConn := clientQ.conn
	clientQ.mu.Unlock()
	if clientConn == nil {
		t.Fatal("Client did not establish connection")
	}

	// Create a dummy task
	taskToPush := &DummyTask{
		ID:   "task-123",
		Data: "some test data",
	}

	// Push the task from client
	ctxPush, cancelPush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPush()
	err = clientQ.Push(ctxPush, taskToPush)
	if err != nil {
		t.Fatalf("Failed to push task: %v", err)
	}

	// Pop the task from server
	ctxPop, cancelPop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPop()
	poppedTask, err := serverQ.Pop(ctxPop)
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
// - Client reconnection logic
// - Handling connection closure during Push/Pop
// - Error handling for invalid messages (e.g., wrong magic number, invalid JSON)
// - Concurrent Push/Pop operations (if applicable for the intended use case)
// - Len() method (should always return 0 for this implementation)
// - Close() method behavior
