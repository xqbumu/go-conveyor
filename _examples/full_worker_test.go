package _examples

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/xqbumu/go-conveyor"
)

// FullWorker is a simple implementation of the Worker interface.
type FullWorker struct {
	manager *conveyor.Manager
	index   atomic.Int32
}

// Produce implements the Produce method of the Worker interface.
func (w *FullWorker) Produce(ctx context.Context) error {
	w.index.Add(1)
	return w.manager.AddTask(
		ctx,
		fmt.Sprintf("auto_task-%d", w.index),
		"full_task",
		fmt.Sprintf("Hello, Auto task %d!", w.index),
		0,
		0,
	)
}

// Consume implements the Consume method of the Worker interface.
func (w *FullWorker) Consume(ctx context.Context, task conveyor.Task) error {
	select {
	case <-ctx.Done():
		log.Printf("Task %s (Type: %s) cancelled or timed out\n", task.ID, task.Type)
		return ctx.Err()
	default:
		log.Printf("Processing task %s (Type: %s, Priority: %d, Data: %v)\n", task.ID, task.Type, task.Priority, task.Data)
		time.Sleep(50 * time.Millisecond)
		return nil
	}
}

// ProduceInterval returns the interval for calling the Produce method.
func (w *FullWorker) ProduceCronSchedule() (string, bool) {
	return `*/1 * * * * *`, true
}

// Types returns the list of task types that the Worker can handle.
func (w *FullWorker) Types(ctx context.Context) []conveyor.Type {
	return []conveyor.Type{"full_task"}
}

func ExampleNewManager_full() {
	ctx, cancel := context.WithCancel(context.Background())

	manager, err := conveyor.NewManager(
		ctx,
		conveyor.WithDefaultBufferSize(10),
		conveyor.WithDefaultConsumers(2),
		conveyor.WithMaxTotalConsumers(5),
	)
	if err != nil {
		log.Printf("Failed to create TaskManager: %v\n", err)
		return
	}

	manager.Register(&FullWorker{manager: manager, index: atomic.Int32{}})

	go manager.Start()

	manager.AddTask(context.TODO(), "manual_task-1", "full_task", "Hello, Manual task 1!", 0, 0)
	manager.AddTask(context.TODO(), "manual_task-2", "full_task", "Hello, Manual task 2!", 1, 0)
	manager.AddTask(context.TODO(), "manual_task-3", "full_task", "Hello, Manual task 3!", 0, 0)

	time.Sleep(1500 * time.Millisecond)
	cancel()
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("Metrics: %+v\n", manager.GetMetrics())

	// Output:
	// Metrics: {TasksQueued:0 TasksProcessed:5 TasksFailed:0 WorkersActive:0}
}
