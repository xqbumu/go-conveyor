package _examples

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/xqbumu/go-conveyor"
)

// FaultyWorker is a Worker that simulates panic and errors.
type FaultyWorker struct {
	attempts map[string]int // Stores the number of attempts for each task ID
	mu       sync.Mutex     // Protects the attempts map
}

// NewFaultyWorker creates a new instance of FaultyWorker
func NewFaultyWorker() *FaultyWorker {
	return &FaultyWorker{
		attempts: make(map[string]int),
	}
}

// getAttemptCount gets and increments the attempt count for the specified task ID
func (w *FaultyWorker) getAttemptCount(taskID string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempts[taskID]++
	return w.attempts[taskID]
}

// Produce implements the Produce method of the Worker interface
func (w *FaultyWorker) Produce(ctx context.Context) error {
	// This Worker does not automatically generate tasks
	return nil
}

// Consume implements the Consume method of the Worker interface
func (w *FaultyWorker) Consume(ctx context.Context, t conveyor.Task) error {
	select {
	case <-ctx.Done():
		log.Printf("Faulty Task %s (Type: %s) cancelled or timed out", t.ID, t.Type)
		return ctx.Err()
	default:
		attempt := w.getAttemptCount(t.ID) // Get and increment the attempt count for the current task
		log.Printf("Processing faulty task %s (Attempt: %d)", t.ID, attempt)

		switch t.ID {
		case "panic-task":
			// Panic on the first attempt
			if attempt == 1 {
				log.Printf("Faulty task %s panicking on attempt %d", t.ID, attempt)
				panic("simulated panic")
			}
			// Succeed on the second attempt (after panic recovery)
			log.Printf("Faulty task %s succeeded on attempt %d after panic recovery", t.ID, attempt)
			return nil
		case "retry-task":
			// Fail on the first two attempts, succeed on the third attempt (TaskManager retries 3 times by default)
			if attempt <= 2 {
				log.Printf("Faulty task %s failing on attempt %d, will retry", t.ID, attempt)
				return fmt.Errorf("simulated error on attempt %d", attempt)
			}
			log.Printf("Faulty task %s succeeded on attempt %d after retries", t.ID, attempt)
			return nil
		case "success-task":
			// Always succeed
			log.Printf("Faulty task %s succeeded on attempt %d", t.ID, attempt)
			return nil
		default:
			// Unknown task type, return an error
			log.Printf("Faulty task %s received unknown ID", t.ID)
			return fmt.Errorf("unknown task ID: %s", t.ID)
		}
	}
}

// Types implements the Types method of the Worker interface
func (w *FaultyWorker) Types(ctx context.Context) []conveyor.Type {
	return []conveyor.Type{"faulty_task"}
}

// ExampleNewManager_faulty tests the panic recovery and retry logic of TaskManager
func ExampleNewManager_faulty() {
	// Create a context to control the lifecycle of TaskManager
	ctx, cancel := context.WithCancel(context.Background())

	// Create TaskManager with a smaller buffer size and number of consumers to observe behavior
	manager, err := conveyor.NewManager(
		ctx,
		conveyor.WithDefaultBufferSize(5),
		conveyor.WithDefaultConsumers(2),
		conveyor.WithMaxTotalConsumers(5),
		conveyor.WithRetryOnPanic(true),
	)
	if err != nil {
		log.Fatalf("Failed to create TaskManager: %v", err)
	}

	// Register FaultyWorker
	faultyWorker := NewFaultyWorker() // Create an instance using the constructor
	manager.Register(faultyWorker)

	// Start TaskManager in a goroutine
	go manager.Start()

	// Wait for TaskManager to start
	time.Sleep(100 * time.Millisecond)

	// Add test tasks
	manager.AddTask(context.TODO(), "panic-task", "faulty_task", "This task will panic", 0, 0)
	manager.AddTask(context.TODO(), "retry-task", "faulty_task", "This task will retry", 0, 0)
	manager.AddTask(context.TODO(), "success-task", "faulty_task", "This task will succeed", 0, 0)
	manager.AddTask(context.TODO(), "unknown-task", "faulty_task", "This task has unknown ID", 0, 0) // Test unknown task ID

	// Wait for all tasks to complete
	// Allow enough time for panic recovery and retries to occur
	time.Sleep(5 * time.Second) // Extend the wait time appropriately

	// Stop TaskManager
	cancel()
	time.Sleep(1 * time.Second) // Wait for the stop to complete

	// Check metrics
	metrics := manager.GetMetrics()
	fmt.Printf("Final Metrics: %+v", metrics)

	// Expected results:
	// panic-task: Panic on the first attempt, succeed on the second attempt after recovery -> 1 processed
	// retry-task: Fail on the first attempt, fail on the second attempt, succeed on the third attempt (TaskManager retries 3 times by default) -> 1 processed
	// success-task: Succeed on the first attempt -> 1 processed
	// unknown-task: Fail on the first attempt, retry 3 times, and finally fail -> 1 failed
	// Total: 3 processed, 1 failed, 0 queued

	expectedProcessed := int64(3)
	expectedFailed := int64(1)
	expectedQueued := int64(0)

	if metrics.TasksProcessed != expectedProcessed {
		log.Fatalf("Expected TasksProcessed: %d, Got: %d", expectedProcessed, metrics.TasksProcessed)
	}
	if metrics.TasksFailed != expectedFailed {
		log.Fatalf("Expected TasksFailed: %d, Got: %d", expectedFailed, metrics.TasksFailed)
	}
	if metrics.TasksQueued != expectedQueued {
		log.Fatalf("Expected TasksQueued: %d, Got: %d", expectedQueued, metrics.TasksQueued)
	}

	// Output:
	// Final Metrics: {TasksQueued:0 TasksProcessed:3 TasksFailed:1 WorkersActive:0}
}
