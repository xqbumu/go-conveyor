package conveyor

import (
	"context"
	"fmt"
	"time"
)

// Worker defines the interface for task processing units.
// Each Worker can handle one or more types of tasks.
// It contains two core methods: Produce for generating tasks (optional), and Consume for processing tasks.
type Worker interface {
	// Produce periodically generates tasks to be processed, called by the TaskManager's timer.
	// If the Worker does not need to automatically generate tasks, it can be implemented as an empty method.
	Produce(ctx context.Context) error
	// Consume processes the specified type of task.
	Consume(ctx context.Context, task ITask) error
	// Types returns the list of task types that the Worker can handle.
	Types(ctx context.Context) []any
}

// WorkerProduceCronSchedule is an optional interface that Workers can implement
// to specify a cron schedule for their Produce method.
type WorkerProduceCronSchedule interface {
	// ProduceCronSchedule returns the desired cron schedule string for calling the Produce method.
	// If the returned string is empty or the boolean is false, the global cron schedule will be used.
	ProduceCronSchedule() (string, bool)
}

type ITask interface {
	GetIdentify() string
	GetID() string
	GetType() any
	GetPriority() int
	GetTimeout() time.Duration
}

type TaskData[T any] interface {
	Data() T
}

// Task defines a task, containing all the information needed to execute the task.
type Task[T any] struct {
	ID       string        // Unique ID of the task, used for deduplication and tracking.
	Typ      any           // Type of the task, used to distribute to the corresponding Worker.
	Priority int           // Task priority (0: normal, >0: high priority, high priority tasks will try to be processed faster).
	Timeout  time.Duration // Task execution timeout (0 means no timeout).
	Data     T             // Data carried by the task, the specific type is determined by the task type.
}

func NewTask[T any](id string, typ any, priority int, timeout time.Duration, data T) Task[T] {
	return Task[T]{
		ID:       id,
		Typ:      typ,
		Priority: priority,
		Timeout:  timeout,
		Data:     data,
	}
}

// Identify returns the unique identifier of the task.
func (t Task[T]) GetIdentify() string {
	return fmt.Sprintf("%s:%s", t.ID, t.Typ)
}

// GetID returns the unique GetID of the task.
func (t Task[T]) GetID() string {
	return t.ID
}

// Type returns the type of the task.
func (t Task[T]) GetType() any {
	return t.Typ
}

// Priority returns the priority of the task.
func (t Task[T]) GetPriority() int {
	return t.Priority
}

// Timeout returns the timeout duration of the task.
func (t Task[T]) GetTimeout() time.Duration {
	return t.Timeout
}

// Data returns the data associated with the task.
func (t Task[T]) GetData() T {
	return t.Data
}

// PanicError wraps a panic value to be returned as an error.
type PanicError struct {
	Value any
}

func (e PanicError) Error() string {
	return fmt.Sprintf("task panicked: %v", e.Value)
}

// TaskQueue defines the interface for task storage and retrieval mechanisms.
type TaskQueue interface {
	// Push adds a task to the queue.
	Push(ctx context.Context, task ITask) error
	// Pop retrieves a task from the queue, blocking until a task is available or the context is done.
	Pop(ctx context.Context) (ITask, error)
	// Len returns the current number of tasks in the queue.
	Len() int
	// Close closes the queue, preventing further pushes and allowing existing tasks to be processed.
	Close() error
}

// TaskQueueFactory defines a function type for creating TaskQueue instances.
// It takes the priority and buffer size as input and returns a TaskQueue implementation and an error.
type TaskQueueFactory func(priority int, bufferSize int) (TaskQueue, error)
