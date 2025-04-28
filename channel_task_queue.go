package conveyor

import (
	"context"
	"errors"
	"log/slog"
)

// ChannelTaskQueue implements the TaskQueue interface using a Go channel.
type ChannelTaskQueue struct {
	tasks chan ITask
}

// NewChannelTaskQueue creates a new ChannelTaskQueue with the specified buffer size.
func NewChannelTaskQueue(bufferSize int) *ChannelTaskQueue {
	return &ChannelTaskQueue{
		tasks: make(chan ITask, bufferSize),
	}
}

// Push adds a task to the channel.
func (q *ChannelTaskQueue) Push(task ITask) error {
	select {
	case q.tasks <- task:
		slog.Debug("Task pushed to channel", "identifier", task.GetIdentify())
		return nil
	default:
		// This case should ideally not be hit with a buffered channel unless it's full.
		// For an unbuffered channel, this would mean no receiver is ready.
		// Given our use case with a buffer, this indicates the buffer is full.
		err := errors.New("task channel is full")
		slog.Error("Failed to push task to channel", "identifier", task.GetIdentify(), "error", err)
		return err
	}
}

// Pop retrieves a task from the channel, blocking until a task is available or the context is done.
func (q *ChannelTaskQueue) Pop(ctx context.Context) (ITask, error) {
	select {
	case task, ok := <-q.tasks:
		if !ok {
			slog.Info("Task channel is closed, Pop returning error")
			var zero ITask
			return zero, errors.New("task channel is closed")
		}
		slog.Debug("Task popped from channel", "identifier", task.GetIdentify())
		return task, nil
	case <-ctx.Done():
		slog.Info("Context done, Pop returning context error")
		var zero ITask
		return zero, ctx.Err()
	}
}

// Len returns the current number of tasks in the channel.
func (q *ChannelTaskQueue) Len() int {
	return len(q.tasks)
}

// Close closes the channel.
func (q *ChannelTaskQueue) Close() error {
	slog.Info("Closing task channel")
	close(q.tasks)
	return nil
}
