package _examples

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/xqbumu/go-conveyor"
)

// SimpleWorker is a simple Worker implementation for processing string tasks.
type SimpleWorker struct{}

// Produce implements the Produce method of the Worker interface.
func (w *SimpleWorker) Produce(ctx context.Context) error {
	// This simple Worker does not automatically generate tasks.
	return nil
}

// Consume implements the Consume method of the Worker interface.
func (w *SimpleWorker) Consume(ctx context.Context, t conveyor.Task) error {
	select {
	case <-ctx.Done():
		log.Printf("Task %s (Type: %s) cancelled or timed out", t.ID, t.Type)
		return ctx.Err()
	default:
		// Simulate task processing.
		log.Printf("Processing task %s (Type: %s, Priority: %d, Data: %v)", t.ID, t.Type, t.Priority, t.Data)
		time.Sleep(50 * time.Millisecond) // Simulate work.
		// Actual processing can be done here based on t.Data.
		return nil
	}
}

// Types implements the Types method of the Worker interface.
func (w *SimpleWorker) Types(ctx context.Context) []conveyor.Type {
	return []conveyor.Type{"simple_string_task"}
}

// ExampleNewManager demonstrates how to create and use TaskManager.
func ExampleNewManager_simple() {
	// Create a context to control the lifecycle of the TaskManager.
	ctx, cancel := context.WithCancel(context.Background())

	// Create a TaskManager with configuration options.
	manager, err := conveyor.NewManager(
		ctx,
		conveyor.WithDefaultBufferSize(20),
		conveyor.WithDefaultConsumers(5),
		conveyor.WithMaxTotalConsumers(20),
		conveyor.WithPriorityConsumers(map[int]int{
			1: 2, // Priority 1 tasks use 2 consumers.
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create TaskManager: %v", err)
	}

	// Register Worker.
	simpleWorker := &SimpleWorker{}
	manager.Register(simpleWorker)

	// Start the TaskManager in a goroutine.
	go manager.Start()

	// Wait for the TaskManager to start (optional, ensure channels are ready).
	time.Sleep(100 * time.Millisecond)

	// Add some tasks.
	manager.AddTask(context.TODO(), "task-1", "simple_string_task", "Hello, Task 1!", 0, 0)
	manager.AddTask(context.TODO(), "task-2", "simple_string_task", "Hello, Task 2!", 1, 0) // High priority task.
	manager.AddTask(context.TODO(), "task-3", "simple_string_task", "Hello, Task 3!", 0, 0)
	manager.AddTask(context.TODO(), "task-4", "simple_string_task", "Hello, Task 4!", 1, 0) // High priority task.
	manager.AddTask(context.TODO(), "task-5", "simple_string_task", "Hello, Task 5!", 0, 0)

	// Wait for a while to let the tasks process.
	time.Sleep(500 * time.Millisecond)

	// Stop the TaskManager.
	cancel()                           // Cancel the context to trigger TaskManager stop.
	time.Sleep(200 * time.Millisecond) // Wait for stop to complete.

	// Get and print metrics.
	metrics := manager.GetMetrics()
	fmt.Printf("Metrics: %+v\n", metrics)

	// Expected output: (actual output may vary due to concurrency and scheduling, this is just an example format)
	// Processing task task-2 (Type: simple_string_task, Priority: 1, Data: Hello, Task 2!)
	// Processing task task-4 (Type: simple_string_task, Priority: 1, Data: Hello, Task 4!)
	// Processing task task-1 (Type: simple_string_task, Priority: 0, Data: Hello, Task 1!)
	// Processing task task-3 (Type: simple_string_task, Priority: 0, Data: Hello, Task 3!)
	// Processing task task-5 (Type: simple_string_task, Priority: 0, Data: Hello, Task 5!)

	// Output:
	// Metrics: {TasksQueued:0 TasksProcessed:5 TasksFailed:0 WorkersActive:0}
}
