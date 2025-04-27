package conveyor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ConsumerPoolManager is responsible for managing task channels and consumer goroutine pools.
type ConsumerPoolManager struct {
	ctx          context.Context
	wg           sync.WaitGroup    // Used to wait for all consumer goroutines to finish
	taskChannels map[int]chan Task // Task channel map, key is priority, value is the corresponding task channel
	mu           sync.RWMutex      // Read-write lock to protect concurrent access to taskChannels

	// Resource limitation related fields
	currentConsumers int32                      // Current number of active consumer goroutines
	config           *ConsumerPoolManagerConfig // Configuration for the ConsumerPoolManager
}

// NewConsumerPoolManager creates and initializes a new ConsumerPoolManager instance.
func NewConsumerPoolManager(
	ctx context.Context,
	config *ConsumerPoolManagerConfig,
) *ConsumerPoolManager {
	cpm := &ConsumerPoolManager{
		ctx:          ctx,
		taskChannels: make(map[int]chan Task),
		config:       config,
	}

	// Create a default priority (0) channel with the default buffer size
	// StartDefaultConsumers will be called in NewManager to start default consumers
	cpm.taskChannels[0] = make(chan Task, cpm.config.DefaultBufferSize)
	slog.Info("Created default task channel with initial buffer size", "priority", 0, "bufferSize", cpm.config.DefaultBufferSize)

	slog.Info("ConsumerPoolManager created")
	return cpm
}

// StartDefaultConsumers starts the consumer goroutine pool for the default priority (0).
func (cpm *ConsumerPoolManager) StartDefaultConsumers(processTask processTaskFunc) {
	defaultTaskCh, ok := cpm.taskChannels[0]
	if ok {
		numConsumers := cpm.config.DefaultConsumers
		slog.Info("Starting consumers for default priority (0)", "count", numConsumers)
		for i := 0; i < numConsumers; i++ {
			cpm.wg.Add(1)                             // Increase WaitGroup count
			atomic.AddInt32(&cpm.currentConsumers, 1) // Increase active consumer count
			go cpm.startConsumer(defaultTaskCh, processTask)
		}
	} else {
		slog.Warn("Default task channel (0) not found, no consumers started for default priority.")
	}
}

// GetOrCreateChannel gets the task channel for the specified priority.
// If the channel does not exist, it attempts to create and start the corresponding consumer goroutine pool.
// Returns the task channel and any possible error.
func (cpm *ConsumerPoolManager) GetOrCreateChannel(priority int, taskIdentifier string, processTask processTaskFunc) (chan Task, error) {
	cpm.mu.RLock()
	taskCh, ok := cpm.taskChannels[priority]
	cpm.mu.RUnlock()

	if !ok {
		// Channel does not exist, need to create and start consumers
		cpm.mu.Lock()
		// Check again to prevent other goroutines from creating it
		taskCh, ok = cpm.taskChannels[priority]
		if !ok {
			// Create a new channel with the default buffer size
			defaultBufferSize := cpm.config.DefaultBufferSize

			// Check if the maximum number of priority channels (excluding default priority 0) is exceeded
			if priority != 0 && int32(len(cpm.taskChannels)-1) >= cpm.config.MaxPriorityChannels {
				cpm.mu.Unlock()
				err := fmt.Errorf("max priority channel limit (%d) reached, cannot create channel for priority %d", cpm.config.MaxPriorityChannels, priority)
				slog.Error("Failed to get or create channel", "identifier", taskIdentifier, "priority", priority, "error", err)
				return nil, err
			}

			taskCh = make(chan Task, defaultBufferSize)
			cpm.taskChannels[priority] = taskCh
			slog.Info("Dynamically created task channel", "priority", priority, "bufferSize", defaultBufferSize)

			// Determine the number of consumers for the newly created channel
			numConsumersToStart, ok := cpm.config.PriorityConsumers[priority]
			if !ok || numConsumersToStart <= 0 {
				numConsumersToStart = cpm.config.DefaultConsumers // Use default consumer count if not configured or invalid
			}

			// Check if the maximum total number of consumer goroutines is exceeded
			if atomic.LoadInt32(&cpm.currentConsumers)+int32(numConsumersToStart) > cpm.config.MaxTotalConsumers {
				cpm.mu.Unlock()
				delete(cpm.taskChannels, priority) // Delete the newly created channel
				err := fmt.Errorf("max total consumers limit (%d) reached, cannot start %d consumers for priority %d", cpm.config.MaxTotalConsumers, numConsumersToStart, priority)
				slog.Error("Failed to get or create channel", "identifier", taskIdentifier, "priority", priority, "error", err)
				return nil, err
			}

			slog.Info("Starting consumers for dynamically created priority", "priority", priority, "count", numConsumersToStart)
			for i := 0; i < numConsumersToStart; i++ {
				cpm.wg.Add(1)                             // Increase WaitGroup count
				atomic.AddInt32(&cpm.currentConsumers, 1) // Increase active consumer count
				// Note: The TaskManager's startup context should be passed here, not the AddTask context
				// Because the lifecycle of the consumer goroutine should be controlled by the TaskManager
				go cpm.startConsumer(taskCh, processTask)
			}
		}
		cpm.mu.Unlock()
	}

	return taskCh, nil
}

// startConsumer is the function executed by each consumer goroutine.
// It continuously reads tasks from the specified task channel (taskCh) and distributes them to the processTaskFunc for processing.
// Supports task timeout, cancellation, retry mechanisms, and includes panic recovery and limited restart logic.
func (cpm *ConsumerPoolManager) startConsumer(taskCh <-chan Task, processTask processTaskFunc) {
	// Ensure that the WaitGroup is notified and the active consumer count is updated when the goroutine exits
	defer cpm.wg.Done()
	defer atomic.AddInt32(&cpm.currentConsumers, -1)

	restartAttempts := 0
	maxRestartAttempts := 5            // Define the maximum number of automatic restarts
	initialRestartDelay := time.Second // Define the initial restart interval
	currentRestartDelay := initialRestartDelay

	// The outer loop handles the restart of the goroutine.
	// If the inner task processing loop exits due to panic or channel closure, the outer loop will attempt to restart.
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
			// Task processing loop: continuously receive tasks from the task channel until the channel is closed.
			for task := range taskCh {
				// Before processing each task, check again if the context is canceled.
				// This allows for quick exit from task processing when the TaskManager is stopping.
				select {
				case <-cpm.ctx.Done():
					slog.Info("Consumer goroutine received context done during task processing, exiting inner loop")
					return // TaskManager is stopping, exit the inner function
				default:
					// TaskManager is not stopping, continue processing the current task.
				}

				// Task dequeued, update queued count.
				// Note: The TasksQueued metric should now be maintained by the TaskManager
				// atomic.AddInt64(&m.metrics.TasksQueued, -1)

				// Call the external task processing function
				processTask(cpm.ctx, task)

				// Note: The TasksProcessed and TasksFailed metrics should now be maintained by the TaskManager or within the processTaskFunc
				// atomic.AddInt64(&m.metrics.TasksProcessed, 1)
				// atomic.AddInt64(&m.metrics.TasksFailed, 1)
			}
			slog.Info("Consumer goroutine task channel closed, exiting inner loop")
			// If the channel is closed, the inner function exits normally.
		}() // End of inner anonymous function

		// If the inner function exits (whether normally or after panic recovery),
		// check if the TaskManager's context has been canceled.
		select {
		case <-cpm.ctx.Done():
			slog.Info("Consumer goroutine detected context done after inner loop exit, exiting goroutine")
			return // TaskManager is stopping, exit the goroutine
		default:
			// The inner function exited, but the TaskManager is not stopping.
			// This usually means a panic occurred or the channel was closed in a non-Stop state (exceptional case).
			// Attempt to restart the goroutine.
			restartAttempts++
			if restartAttempts > maxRestartAttempts {
				slog.Error("Consumer goroutine reached max restart attempts, giving up", "maxAttempts", maxRestartAttempts)
				return // Reached the maximum number of restart attempts, give up and exit the goroutine
			}

			slog.Info("Attempting to restart consumer goroutine", "attempt", restartAttempts, "delay", currentRestartDelay)
			// Wait for a period of time before attempting to restart, using exponential backoff.
			time.Sleep(currentRestartDelay)
			currentRestartDelay *= 2 // Exponential backoff increases the delay
			// The outer loop continues, starting the next inner function execution, i.e., restarting the goroutine.
		}
	} // End of outer restart loop
}

// Stop gracefully stops all consumer goroutines and closes task channels.
func (cpm *ConsumerPoolManager) Stop() {
	slog.Info("Stopping ConsumerPoolManager components...")
	// Close all task channels
	cpm.mu.Lock() // Lock to protect concurrent access to taskChannels
	for priority, taskCh := range cpm.taskChannels {
		close(taskCh)
		slog.Info("Task channel closed", "priority", priority)
	}
	cpm.mu.Unlock()
	slog.Info("All task channels closed")
	// Wait for all consumer goroutines to complete their current tasks and exit
	slog.Info("Waiting for consumers to finish...")
	cpm.wg.Wait()
	slog.Info("All consumers finished")
}

// processTaskFunc defines a function type for processing a single task.
// This function type will be passed to startConsumer so that the ConsumerPoolManager
// can call the actual task processing logic (processTaskWithRetry) defined in the TaskManager.
type processTaskFunc func(ctx context.Context, task Task)

// GetCurrentConsumers returns the current number of active consumer goroutines.
func (cpm *ConsumerPoolManager) GetCurrentConsumers() int32 {
	return atomic.LoadInt32(&cpm.currentConsumers)
}
