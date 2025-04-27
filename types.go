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
	Consume(ctx context.Context, task Task) error
	// Types returns the list of task types that the Worker can handle.
	Types(ctx context.Context) []Type
}

// Type defines the type of task, using a string for easy extension.
type Type string

// Task defines a task, containing all the information needed to execute the task.
type Task struct {
	ID       string        // Unique ID of the task, used for deduplication and tracking.
	Type     Type          // Type of the task, used to distribute to the corresponding Worker.
	Data     any           // Data carried by the task, the specific type is determined by the task type.
	Priority int           // Task priority (0: normal, >0: high priority, high priority tasks will try to be processed faster).
	Timeout  time.Duration // Task execution timeout (0 means no timeout).
}

// Identify returns the unique identifier of the task.
func (t Task) Identify() string {
	return fmt.Sprintf("%s:%s", t.ID, t.Type)
}

// PanicError wraps a panic value to be returned as an error.
type PanicError struct {
	Value any
}

func (e PanicError) Error() string {
	return fmt.Sprintf("task panicked: %v", e.Value)
}
