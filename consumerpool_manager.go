package conveyor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ConsumerPoolManager is responsible for managing task queues and consumer goroutine pools.
type ConsumerPoolManager struct {
	ctx        context.Context
	wg         sync.WaitGroup    // Used to wait for all consumer goroutines to finish
	taskQueues map[int]TaskQueue // Task queue map, key is priority, value is the corresponding TaskQueue implementation
	mu         sync.RWMutex      // Read-write lock to protect concurrent access to taskQueues

	// Resource limitation related fields
	currentConsumers int32                      // Current number of active consumer goroutines
	config           *ConsumerPoolManagerConfig // Configuration for the ConsumerPoolManager
	taskQueueFactory TaskQueueFactory           // Factory function to create TaskQueue instances
}

// NewConsumerPoolManager creates and initializes a new ConsumerPoolManager instance.
// It takes a TaskQueueFactory function to create the appropriate TaskQueue implementation.
func NewConsumerPoolManager(
	ctx context.Context,
	config *ConsumerPoolManagerConfig,
	taskQueueFactory TaskQueueFactory, // Add TaskQueueFactory parameter
) *ConsumerPoolManager {
	cpm := &ConsumerPoolManager{
		ctx:              ctx,
		taskQueues:       make(map[int]TaskQueue),
		config:           config,
		taskQueueFactory: taskQueueFactory, // Store the factory
	}

	// Create a default priority (0) task queue using the factory
	defaultQueue, err := cpm.taskQueueFactory(0, cpm.config.DefaultBufferSize)
	if err != nil {
		slog.Error("Failed to create default task queue", "error", err)
		return nil // Return nil if default queue creation fails
	}
	cpm.taskQueues[0] = defaultQueue
	slog.Info("Created default task queue with initial buffer size", "priority", 0, "bufferSize", cpm.config.DefaultBufferSize)

	slog.Info("ConsumerPoolManager created")
	return cpm
}

// StartDefaultConsumers starts the consumer goroutine pool for the default priority (0).
func (cpm *ConsumerPoolManager) StartDefaultConsumers(processTask processTaskFunc) {
	defaultTaskQueue, ok := cpm.taskQueues[0]
	if ok {
		numConsumers := cpm.config.DefaultConsumers
		slog.Info("Starting consumers for default priority (0)", "count", numConsumers)
		for i := 0; i < numConsumers; i++ {
			cpm.wg.Add(1)                             // Increase WaitGroup count
			atomic.AddInt32(&cpm.currentConsumers, 1) // Increase active consumer count
			go cpm.startConsumer(defaultTaskQueue, processTask)
		}
	} else {
		slog.Warn("Default task queue (0) not found, no consumers started for default priority.")
	}
}

// GetOrCreateQueue gets the task queue for the specified priority.
// If the queue does not exist, it attempts to create and start the corresponding consumer goroutine pool.
// Returns the task queue and any possible error.
func (cpm *ConsumerPoolManager) GetOrCreateQueue(priority int, taskIdentifier string, processTask processTaskFunc) (TaskQueue, error) {
	cpm.mu.RLock()
	taskQueue, ok := cpm.taskQueues[priority]
	cpm.mu.RUnlock()

	if !ok {
		// Queue does not exist, need to create and start consumers
		cpm.mu.Lock()
		defer cpm.mu.Unlock() // Ensure unlock in case of early return

		// Check again to prevent other goroutines from creating it
		taskQueue, ok = cpm.taskQueues[priority]
		if !ok {
			// Create a new queue with the default buffer size
			defaultBufferSize := cpm.config.DefaultBufferSize

			// Check if the maximum number of priority queues (excluding default priority 0) is exceeded
			if priority != 0 && int32(len(cpm.taskQueues)-1) >= cpm.config.MaxPriorityChannels { // Using MaxPriorityChannels config for now
				err := fmt.Errorf("max priority queue limit (%d) reached, cannot create queue for priority %d", cpm.config.MaxPriorityChannels, priority)
				slog.Error("Failed to get or create queue", "identifier", taskIdentifier, "priority", priority, "error", err)
				return nil, err
			}

			// Create the new queue using the factory
			var err error
			taskQueue, err = cpm.taskQueueFactory(priority, defaultBufferSize)
			if err != nil {
				slog.Error("Failed to create task queue using factory", "priority", priority, "error", err)
				return nil, fmt.Errorf("failed to create task queue for priority %d: %w", priority, err)
			}

			cpm.taskQueues[priority] = taskQueue
			slog.Info("Dynamically created task queue using factory", "priority", priority, "bufferSize", defaultBufferSize)

			// Determine the number of consumers for the newly created queue
			numConsumersToStart, ok := cpm.config.PriorityConsumers[priority]
			if !ok || numConsumersToStart <= 0 {
				numConsumersToStart = cpm.config.DefaultConsumers // Use default consumer count if not configured or invalid
			}

			// Check if the maximum total number of consumer goroutines is exceeded
			if atomic.LoadInt32(&cpm.currentConsumers)+int32(numConsumersToStart) > cpm.config.MaxTotalConsumers {
				delete(cpm.taskQueues, priority) // Delete the newly created queue
				// Close the queue that was just created before deleting it from the map
				if closeErr := taskQueue.Close(); closeErr != nil {
					slog.Error("Failed to close dynamically created queue after exceeding max total consumers", "priority", priority, "error", closeErr)
				}
				err := fmt.Errorf("max total consumers limit (%d) reached, cannot start %d consumers for priority %d", cpm.config.MaxTotalConsumers, numConsumersToStart, priority)
				slog.Error("Failed to get or create queue", "identifier", taskIdentifier, "priority", priority, "error", err)
				return nil, err
			}

			slog.Info("Starting consumers for dynamically created priority", "priority", priority, "count", numConsumersToStart)
			for i := 0; i < numConsumersToStart; i++ {
				cpm.wg.Add(1)                             // Increase WaitGroup count
				atomic.AddInt32(&cpm.currentConsumers, 1) // Increase active consumer count
				// Note: The TaskManager's startup context should be passed here, not the AddTask context
				// Because the lifecycle of the consumer goroutine should be controlled by the TaskManager
				go cpm.startConsumer(taskQueue, processTask)
			}
		}
		// cpm.mu.Unlock() // Removed defer unlock, added explicit unlock or defer
	}

	return taskQueue, nil
}

// startConsumer is the function executed by each consumer goroutine.
// It continuously reads tasks from the specified task queue (taskQueue) and distributes them to the processTaskFunc for processing.
// Supports task timeout, cancellation, retry mechanisms, and includes panic recovery and limited restart logic.
func (cpm *ConsumerPoolManager) startConsumer(taskQueue TaskQueue, processTask processTaskFunc) {
	// Ensure that the WaitGroup is notified and the active consumer count is updated when the goroutine exits
	defer cpm.wg.Done()
	defer atomic.AddInt32(&cpm.currentConsumers, -1)

	restartAttempts := 0
	maxRestartAttempts := 5            // Define the maximum number of automatic restarts
	initialRestartDelay := time.Second // Define the initial restart interval
	currentRestartDelay := initialRestartDelay

	// The outer loop handles the restart of the goroutine.
	// If the inner task processing loop exits due to panic or queue closure/error, the outer loop will attempt to restart.
	for {
		// Before each restart attempt, check if the TaskManager's context has been canceled.
		// If the TaskManager is stopping, exit the entire goroutine.
		select {
		case <-cpm.ctx.Done():
			slog.Info("Consumer goroutine received context done, exiting restart loop")
			return // TaskManager is stopping, exit the goroutine
		default:
			// TaskManager is not stopping, continue executing the inner task processing logic.
		}

		// The inner anonymous function contains the actual task processing loop and panic recovery.
		// Using an anonymous function and defer recover() is the standard pattern in Go for catching panics.
		func() {
			// defer recover() catches panics in the current goroutine.
			defer func() {
				if r := recover(); r != nil {
					// Log the panic error and attempt to restart.
					slog.Error("Consumer goroutine panicked, attempting restart",
						"panic", r,
						"attempt", restartAttempts+1,
						"maxAttempts", maxRestartAttempts,
						"delay", currentRestartDelay)
					// Panic is caught, the function will return here, and the outer loop will detect it and possibly restart.
				}
			}()

			slog.Debug("Consumer goroutine started processing loop")
			// Task processing loop: continuously receive tasks from the task queue until the queue is closed and empty.
			for {
				// Before processing each task, check again if the context is canceled.
				// This allows for quick exit from task processing when the TaskManager is stopping.
				select {
				case <-cpm.ctx.Done():
					slog.Info("Consumer goroutine received context done during task processing, exiting inner loop")
					return // TaskManager is stopping, exit the inner function
				default:
					// TaskManager is not stopping, continue processing the current task.
				}

				task, err := taskQueue.Pop(cpm.ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						slog.Info("Consumer goroutine context canceled or deadline exceeded during Pop, exiting inner loop")
						return // Context canceled, exit the inner function
					}
					// If the queue is closed and empty, Pop might return a specific error (e.g., io.EOF or a custom error).
					// We need to handle this case to exit the loop gracefully.
					// For ChannelTaskQueue, the range loop handles this, but with the interface, we check the error.
					// Assuming a closed queue returns an error after processing all tasks.
					slog.Info("Consumer goroutine received error from Pop, exiting inner loop", "error", err)
					return // Exit the inner function on other errors from Pop
				}

				// Task dequeued, update queued count.
				// Note: The TasksQueued metric should now be maintained by the TaskManager or the TaskQueue implementation
				// atomic.AddInt64(&m.metrics.TasksQueued, -1)

				// Call the external task processing function
				processTask(cpm.ctx, task)

				// Note: The TasksProcessed and TasksFailed metrics should now be maintained by the TaskManager or within the processTaskFunc
				// atomic.AddInt64(&m.metrics.TasksProcessed, 1)
				// atomic.AddInt64(&m.metrics.TasksFailed, 1)
			}
			// slog.Info("Consumer goroutine task channel closed, exiting inner loop") // This log might not be accurate anymore
			// If the inner function exits (e.g., due to Pop error or context done), the outer loop will handle restart logic.
		}() // End of inner anonymous function

		// If the inner function exits (whether normally or after panic recovery),
		// check if the TaskManager's context has been canceled.
		select {
		case <-cpm.ctx.Done():
			slog.Info("Consumer goroutine detected context done after inner loop exit, exiting goroutine")
			return // TaskManager is stopping, exit the goroutine
		default:
			// The inner function exited, but the TaskManager is not stopping.
			// This usually means a panic occurred or an unhandled error from Pop.
			// Attempt to restart the goroutine.
			restartAttempts++
			if restartAttempts > maxRestartAttempts {
				slog.Error("Consumer goroutine reached max restart attempts, giving up", "maxAttempts", maxRestartAttempts)
				return // Reached the maximum number of restart attempts, give up and exit the goroutine
			}

			slog.Info("Attempting to restart consumer goroutine", "attempt", restartAttempts, "delay", currentRestartDelay)
			// Wait for a period of time before attempting to restart, using exponential backoff and allowing cancellation.
			timer := time.NewTimer(currentRestartDelay)
			select {
			case <-timer.C:
				// Timer expired, continue with restart
			case <-cpm.ctx.Done():
				slog.Info("Consumer goroutine context cancelled during restart backoff, exiting")
				// Attempt to stop the timer and drain if necessary
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return // Manager is stopping
			}
			currentRestartDelay *= 2 // Exponential backoff increases the delay
			// The outer loop continues, starting the next inner function execution, i.e., restarting the goroutine.
		}
	} // End of outer restart loop
}

// Stop gracefully stops all consumer goroutines and closes task queues.
func (cpm *ConsumerPoolManager) Stop() {
	slog.Info("Stopping ConsumerPoolManager components...")
	// Close all task queues
	cpm.mu.Lock() // Lock to protect concurrent access to taskQueues
	for priority, taskQueue := range cpm.taskQueues {
		if err := taskQueue.Close(); err != nil {
			slog.Error("Failed to close task queue", "priority", priority, "error", err)
		} else {
			slog.Info("Task queue closed", "priority", priority)
		}
	}
	cpm.mu.Unlock()
	slog.Info("All task queues closed")
	// Wait for all consumer goroutines to complete their current tasks and exit
	slog.Info("Waiting for consumers to finish...")
	cpm.wg.Wait()
	slog.Info("All consumers finished")
}

// processTaskFunc defines a function type for processing a single task.
// This function type will be passed to startConsumer so that the ConsumerPoolManager
// can call the actual task processing logic (processTaskWithRetry) defined in the TaskManager.
type processTaskFunc func(ctx context.Context, task ITask)

// GetCurrentConsumers returns the current number of active consumer goroutines.
func (cpm *ConsumerPoolManager) GetCurrentConsumers() int32 {
	return atomic.LoadInt32(&cpm.currentConsumers)
}
