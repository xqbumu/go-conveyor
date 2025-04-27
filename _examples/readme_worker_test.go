package _examples

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/xqbumu/go-conveyor"
)

// ReadmeWorker is a simple implementation of the Worker interface.
type ReadmeWorker struct {
	manager *conveyor.Manager
	index   atomic.Int32
}

// Produce implements the Produce method of the Worker interface.
func (w *ReadmeWorker) Produce(ctx context.Context) error {
	w.index.Add(1)
	return w.manager.AddTask(
		ctx,
		fmt.Sprintf("auto_task-%d", w.index),
		"readme_task",
		fmt.Sprintf("Hello, Auto task %d!", w.index),
		0,
		0,
	)
}

// Consume implements the Consume method of the Worker interface.
func (w *ReadmeWorker) Consume(ctx context.Context, task conveyor.Task) error {
	select {
	case <-ctx.Done():
		fmt.Printf("Task %s (Type: %s) cancelled or timed out\n", task.ID, task.Type)
		return ctx.Err()
	default:
		fmt.Printf("Processing task %s (Type: %s, Priority: %d, Data: %v)\n", task.ID, task.Type, task.Priority, task.Data)
		time.Sleep(50 * time.Millisecond)
		return nil
	}
}

func (w *ReadmeWorker) Types(ctx context.Context) []conveyor.Type {
	return []conveyor.Type{"readme_task"}
}

func ExampleNewManager_readme() {
	ctx, cancel := context.WithCancel(context.Background())

	manager, err := conveyor.NewManager(
		ctx,
		conveyor.WithDefaultBufferSize(10),
		conveyor.WithDefaultConsumers(2),
		conveyor.WithMaxTotalConsumers(5),
	)
	if err != nil {
		fmt.Printf("Failed to create TaskManager: %v\n", err)
		return
	}

	manager.Register(&ReadmeWorker{manager: manager, index: atomic.Int32{}})

	go manager.Start()

	// manager.AddTask(context.TODO(), "manual_task-1", "readme_task", "Hello, Manual task 1!", 0, 0)
	// manager.AddTask(context.TODO(), "manual_task-2", "readme_task", "Hello, Manual task 2!", 1, 0)
	// manager.AddTask(context.TODO(), "manual_task-3", "readme_task", "Hello, Manual task 3!", 0, 0)

	time.Sleep(5 * time.Second)
	cancel()
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("Metrics: %+v\n", manager.GetMetrics())

	// Output:
	// Metrics: {TasksQueued:0 TasksProcessed:6 TasksFailed:0 WorkersActive:0}
}
