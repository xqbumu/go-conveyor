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
	idx := w.index.Add(1)
	return w.manager.AddTask(
		ctx,
		conveyor.NewTask(
			fmt.Sprintf("auto_task-%d", idx),
			"full_task",
			0,
			0,
			fmt.Sprintf("Hello, Auto task %d!", idx),
		),
	)
}

// Consume implements the Consume method of the Worker interface.
func (w *FullWorker) Consume(ctx context.Context, task conveyor.ITask) error {
	t, ok := task.(conveyor.Task[string])
	if !ok {
		return fmt.Errorf("task is not of type *conveyor.Task")
	}
	select {
	case <-ctx.Done():
		log.Printf("Task %s (Type: %s) cancelled or timed out\n", t.GetID(), t.GetType())
		return ctx.Err()
	default:
		log.Printf("Processing task %s (Type: %s, Priority: %d, Data: %v)\n", t.GetID(), t.GetType(), t.GetPriority(), t.GetData())
		time.Sleep(50 * time.Millisecond)
		return nil
	}
}

// ProduceInterval returns the interval for calling the Produce method.
func (w *FullWorker) ProduceCronSchedule() (string, bool) {
	return `*/1 * * * * *`, true
}

// Types returns the list of task types that the Worker can handle.
func (w *FullWorker) Types(ctx context.Context) []any {
	return []any{"full_task"}
}

func ExampleNewManager_full() {
	ctx, cancel := context.WithCancel(context.Background())

	manager, err := conveyor.NewGenericManager(
		ctx,
		conveyor.WithDefaultBufferSize(10),
		conveyor.WithDefaultConsumers(2),
		conveyor.WithMaxTotalConsumers(5),
	)
	if err != nil {
		cancel()
		log.Printf("Failed to create TaskManager: %v\n", err)
		return
	}

	manager.Register(&FullWorker{manager: manager, index: atomic.Int32{}})

	go manager.Start()

	manager.AddTask(context.TODO(), conveyor.NewTask("manual_task-1", "full_task", 0, 0, "Hello, Manual task 1!"))
	manager.AddTask(context.TODO(), conveyor.NewTask("manual_task-2", "full_task", 1, 0, "Hello, Manual task 2!"))
	manager.AddTask(context.TODO(), conveyor.NewTask("manual_task-3", "full_task", 0, 0, "Hello, Manual task 3!"))

	time.Sleep(1500 * time.Millisecond)
	cancel()
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("Metrics: %+v\n", manager.GetMetrics())

	// Output:
	// Metrics: {TasksQueued:0 TasksProcessed:5 TasksFailed:0 WorkersActive:0}
}
