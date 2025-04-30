package conveyor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Manager is the task manager responsible for receiving, scheduling, and processing tasks.
type Manager struct {
	ctx                 context.Context      // Context for managing cancellation and timeouts
	workers             []Worker             // List of all registered Worker instances
	workerMap           map[any]Worker       // Mapping from task type to corresponding Worker for quick lookup
	metrics             Metrics              // Metrics for the task manager's operation
	mu                  sync.RWMutex         // Read-write lock to protect concurrent access to workers and workerMap
	taskSetManager      *TaskSetManager      // Task set manager for deduplication and periodic cleanup
	consumerPoolManager *ConsumerPoolManager // Consumer goroutine pool manager
	producerManager     *ProducerManager     // Producer manager
	config              ManagerConfig        // Configuration for the Manager
}

func NewManager(ctx context.Context, opts ...Option) (*Manager, error) {
	manager, err := NewGenericManager(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return manager, nil
}

// NewGenericManager creates and initializes a new TaskManager instance
// using the WithOption pattern to pass in configuration.
func NewGenericManager(ctx context.Context, opts ...Option) (*Manager, error) {
	// Create a ManagerConfig instance with default configuration
	config := ManagerConfig{
		ConsumerPoolManagerConfig: NewConsumerPoolManagerConfig(),

		ProducerCronSchedule: "",          // Default empty cron schedule (no cron trigger)
		ProduceTimeout:       time.Minute, // Default producer Produce method timeout
		RetryOnPanic:         false,       // Default to not retry on panic
		MaxRetries:           3,           // Default max retries to 3
	}

	// Apply the passed-in configuration options to the config struct
	for _, opt := range opts {
		opt(&config)
	}

	// Initialize the TaskManager instance
	m := &Manager{
		ctx:            ctx,
		workers:        make([]Worker, 0),
		workerMap:      make(map[any]Worker),
		taskSetManager: NewTaskSetManager(), // Initialize TaskSetManager
		config:         config,              // Store the final configuration
	}

	// Use the provided TaskQueueFactory if available, otherwise use the default channel factory.
	taskQueueFactory := config.TaskQueueFactory
	if taskQueueFactory == nil {
		slog.Info("No TaskQueueFactory provided, using default channel implementation.")
		taskQueueFactory = func(priority int, bufferSize int) (TaskQueue, error) {
			return NewChannelTaskQueue(bufferSize), nil
		}
	}

	// Initialize ConsumerPoolManager, passing in relevant values from the final configuration and the task queue factory
	m.consumerPoolManager = NewConsumerPoolManager(ctx, &config.ConsumerPoolManagerConfig, taskQueueFactory)
	if m.consumerPoolManager == nil {
		// If consumer pool manager creation fails, and we created a Redis client, close it.
		// This is a simplified approach; proper client lifecycle management might be needed.
		// For now, rely on TaskQueue.Close() called in Manager.Stop().
		return nil, fmt.Errorf("failed to create consumer pool manager")
	}

	// Initialize ProducerManager, passing in the cron schedule, Produce timeout, and a function to get the workers
	m.producerManager = NewProducerManager(
		ctx, //	 Pass in the context for the ProducerManager
		m.config.ProducerCronSchedule,
		m.config.ProduceTimeout, // Pass in the Produce timeout configuration
		m.getWorkers,            // Pass in a method to get the list of workers
	)
	if m.producerManager == nil {
		return nil, fmt.Errorf("failed to create producer manager")
	}

	slog.Info("TaskManager created with applied options",
		"defaultBufferSize", m.config.DefaultBufferSize,
		"defaultConsumers", m.config.DefaultConsumers,
		"maxPriorityChannels", m.config.MaxPriorityChannels,
		"maxTotalConsumers", m.config.MaxTotalConsumers,
		"producerCronSchedule", m.config.ProducerCronSchedule,
		"produceTimeout", m.config.ProduceTimeout,
		"retryOnPanic", m.config.RetryOnPanic,
		"maxRetries", m.config.MaxRetries) // Log MaxRetries
	return m, nil
}

// Start starts the task manager
// This method blocks until the passed-in context ctx is cancelled
// It starts the consumer goroutines and the periodic producer goroutine
func (m *Manager) Start() {
	slog.Info("Starting TaskManager...")

	// Start the consumer goroutine pool for the default priority (0), delegated to ConsumerPoolManager
	// Pass in the processTaskWithRetry function as the task processing logic
	m.consumerPoolManager.StartDefaultConsumers(m.processTaskWithRetry)

	// Start the ProducerManager
	slog.Info("Starting ProducerManager...")
	m.producerManager.Start()

	// Block and wait for the external context ctx to be cancelled
	<-m.ctx.Done()
	slog.Info("TaskManager received stop signal, shutting down...")
	m.Stop() // Call the Stop method for cleanup
	slog.Info("TaskManager stopped gracefully")
}

// rollbackAddTask is a helper function to clean up state before a task fails to send to the channel.
func (m *Manager) rollbackAddTask(taskIdentifier string) {
	atomic.AddInt64(&m.metrics.TasksQueued, -1) // Rollback queued count
	m.taskSetManager.Remove(taskIdentifier)     // Remove from taskSetManager
	slog.Warn("Task addition failed or was cancelled before sending, rolled back state", "identifier", taskIdentifier)
}

// executeTaskConsume is a helper function to execute worker.Consume and recover from panic.
func (m *Manager) executeTaskConsume(taskCtx context.Context, worker Worker, task ITask) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Recover from panic
			err = PanicError{Value: r} // Wrap the panic value in a custom error
			slog.Error("Task panicked during execution",
				"taskID", task.GetID(),
				"type", task.GetType(),
				"panic", r)
		}
	}()
	// Call the actual worker Consume method
	return worker.Consume(taskCtx, task)
}

// Stop gracefully stops the task manager
// Stops all timers, closes task channels, and waits for all running goroutines to complete
func (m *Manager) Stop() {
	slog.Info("Stopping TaskManager components...")

	// Stop the cleanup ticker of TaskSetManager
	if m.taskSetManager != nil {
		m.taskSetManager.Stop()
		slog.Info("TaskSetManager cleanup ticker stopped")
	}
	// Stop the ConsumerPoolManager (close channels and wait for consumers)
	if m.consumerPoolManager != nil {
		m.consumerPoolManager.Stop()
		slog.Info("ConsumerPoolManager stopped")
	}
	// Stop the ProducerManager
	if m.producerManager != nil {
		m.producerManager.Stop()
		slog.Info("ProducerManager stopped")
	}

	// Wait for the producer goroutine to complete and exit - now managed internally by ProducerManager
	// slog.Info("Waiting for producer to finish...")
	// m.wg.Wait() // Only wait for the Manager's WaitGroup (producer)
	// slog.Info("Producer finished")

	slog.Info("TaskManager stopped gracefully")
}

// processTaskWithRetry encapsulates the task processing logic.
// If processing fails, it attempts to re-enqueue the task up to MaxRetries times.
// This function is passed as processTaskFunc to ConsumerPoolManager.
func (m *Manager) processTaskWithRetry(ctx context.Context, task ITask) {
	taskIdentifier := task.GetIdentify() // Get task identifier for logging and metrics update

	// Look up the corresponding Worker based on the task type
	m.mu.RLock() // Read lock needed for reading workerMap
	worker, workerFound := m.workerMap[task.GetType()]
	m.mu.RUnlock()

	// Decrement queued count as we are starting to process this task.
	// If it gets re-enqueued, AddTask will increment it again.
	atomic.AddInt64(&m.metrics.TasksQueued, -1)

	if !workerFound {
		// If no corresponding Worker is found, log an error and mark the task as finally failed
		slog.Error("No worker found for task type, marking as failed", "type", task.GetType(), "taskID", task.GetID())
		atomic.AddInt64(&m.metrics.TasksFailed, 1)
		m.taskSetManager.Remove(taskIdentifier) // Remove from set as it's a final state
		return                                  // Task processing failed permanently
	}

	// Create a context for this specific task execution attempt to support timeout
	// Use the context passed from ConsumerPoolManager, but potentially add a task-specific timeout.
	taskCtx := ctx // Start with the base context
	var cancel context.CancelFunc
	if task.GetTimeout() > 0 {
		taskCtx, cancel = context.WithTimeout(ctx, task.GetTimeout())
		defer cancel() // Ensure cancellation happens if timeout is used
	}
	// No need for WithCancel if no timeout, as the base ctx handles overall cancellation.

	slog.Debug("Attempting task execution", "identifier", taskIdentifier, "retryCount", task.GetRetryCount())
	err := m.executeTaskConsume(taskCtx, worker, task) // Execute the task

	if err == nil {
		// Task succeeded
		atomic.AddInt64(&m.metrics.TasksProcessed, 1)
		slog.Debug("Task processed successfully", "identifier", taskIdentifier, "retryCount", task.GetRetryCount())
		m.taskSetManager.Remove(taskIdentifier) // Remove from set as it's a final state
		return                                  // Success, exit the function
	}

	// Task failed (error or panic)
	slog.Error("Task execution failed",
		"taskID", task.GetID(),
		"type", task.GetType(),
		"retryCount", task.GetRetryCount(),
		"error", err)

	// Check if it's a panic and retry is disabled for panics
	if _, isPanic := err.(PanicError); isPanic && !m.config.RetryOnPanic {
		slog.Error("Task panicked and retryOnPanic is false, marking as failed",
			"taskID", task.GetID(),
			"type", task.GetType(),
			"panicError", err)
		atomic.AddInt64(&m.metrics.TasksFailed, 1)
		m.taskSetManager.Remove(taskIdentifier) // Remove from set as it's a final state
		return                                  // Do not retry
	}

	// Check if the maximum number of retries has been reached
	if task.GetRetryCount() >= m.config.MaxRetries {
		slog.Error("Task failed after reaching max retries",
			"identifier", taskIdentifier,
			"retryCount", task.GetRetryCount(),
			"maxRetries", m.config.MaxRetries)
		atomic.AddInt64(&m.metrics.TasksFailed, 1)
		m.taskSetManager.Remove(taskIdentifier) // Remove from set as it's a final state
		return                                  // Failed permanently
	}

	// Check if the base context (from ConsumerPoolManager) is done before attempting re-enqueue
	select {
	case <-ctx.Done():
		slog.Warn("Base context cancelled before task re-enqueue attempt, marking as failed",
			"taskID", task.GetID(),
			"type", task.GetType(),
			"error", ctx.Err())
		atomic.AddInt64(&m.metrics.TasksFailed, 1)
		m.taskSetManager.Remove(taskIdentifier) // Remove from set as it's a final state
		return                                  // Context cancelled
	default:
		// Context is not done, proceed with re-enqueue
		m.reEnqueueTask(ctx, task) // Attempt to re-enqueue the task
	}
}

// reEnqueueTask increments the task's retry count and attempts to add it back to the queue.
// If re-enqueueing fails, it marks the task as failed.
func (m *Manager) reEnqueueTask(ctx context.Context, task ITask) {
	taskIdentifier := task.GetIdentify()
	task.IncrementRetryCount() // Increment in place

	slog.Info("Re-enqueueing task for retry",
		"identifier", taskIdentifier,
		"newRetryCount", task.GetRetryCount(),
		"maxRetries", m.config.MaxRetries)

	// Use a background context for re-enqueueing to avoid being cancelled by the original task's timeout context.
	// However, respect the overall manager context passed in originally.
	// If the manager is stopping (ctx.Done()), AddTask should ideally handle this.
	// Let's pass the original ctx from processTaskWithRetry (which comes from ConsumerPoolManager).
	// AddTask uses this context for queue operations (like waiting if the queue is full).
	err := m.AddTask(ctx, task) // Re-add the task with incremented retry count

	if err != nil {
		// If re-enqueueing fails (e.g., queue closed, context cancelled during push), mark as failed.
		// AddTask should have already removed it from the taskSetManager in case of failure.
		// The TasksQueued count was decremented when processing started, and AddTask failed to increment it.
		slog.Error("Failed to re-enqueue task, marking as failed",
			"identifier", taskIdentifier,
			"retryCount", task.GetRetryCount(),
			"error", err)
		atomic.AddInt64(&m.metrics.TasksFailed, 1)
		// No need to remove from set or adjust queue count here, AddTask handles it on failure.
	}
	// If AddTask succeeds, it increments TasksQueued, balancing the decrement at the start of processTaskWithRetry.
	// The task remains in the taskSetManager.
}

// AddTask adds a new task to the task manager.
// It handles task deduplication via TaskSetManager and pushes the task to the appropriate queue.
// Parameters:
//   - ctx: Context to control the timeout or cancellation of the queue push operation.
//   - task: The task to add (must implement ITask).
//
// Returns:
//   - error: Returns an error if the task already exists (and wasn't a retry),
//     if getting/creating the queue fails, if pushing to the queue fails,
//     or if the context is cancelled during the operation.
func (m *Manager) AddTask(ctx context.Context, task ITask) error {
	taskIdentifier := task.GetIdentify()
	isRetry := task.GetRetryCount() > 0 // Check if this is a retry attempt

	// 1. Handle TaskSetManager interaction
	// If it's a retry, the task should already be in the set.
	// If it's a new task, try to add it to the set.
	if !isRetry {
		added, err := m.taskSetManager.Add(taskIdentifier)
		if err != nil {
			slog.Error("Failed to add new task to task set manager", "identifier", taskIdentifier, "error", err)
			return fmt.Errorf("failed to add task to task set manager: %s: %w", taskIdentifier, err)
		}
		if !added {
			slog.Warn("New task already exists in set, skipping addition", "identifier", taskIdentifier)
			return fmt.Errorf("task already exists: %s", taskIdentifier)
		}
		// New task successfully added to the set.
	} else {
		// For retries, we assume the task is already in the set.
		// We don't need to call Add again. If it wasn't in the set, something went wrong earlier.
		slog.Debug("Task is a retry, skipping TaskSetManager.Add", "identifier", taskIdentifier, "retryCount", task.GetRetryCount())
	}

	// 2. Get or create the corresponding queue via ConsumerPoolManager
	taskQueue, err := m.consumerPoolManager.GetOrCreateQueue(task.GetPriority(), taskIdentifier, m.processTaskWithRetry)
	if err != nil {
		slog.Error("Failed to get or create task queue", "identifier", taskIdentifier, "priority", task.GetPriority(), "error", err)
		// If getting the queue failed, and it was a *new* task we added to the set, remove it.
		if !isRetry {
			m.taskSetManager.Remove(taskIdentifier)
			slog.Warn("Removed new task from set after failing to get queue", "identifier", taskIdentifier)
		}
		// For retries, leave it in the set - maybe the queue will be available next time? Or should we fail it?
		// Let's fail it permanently if queue acquisition fails during retry.
		if isRetry {
			slog.Error("Failed to get queue during retry, marking task as failed", "identifier", taskIdentifier)
			atomic.AddInt64(&m.metrics.TasksFailed, 1) // Mark as failed
			m.taskSetManager.Remove(taskIdentifier)    // Remove from set
			// No change needed to TasksQueued as it was already decremented by the processing attempt.
		}
		return fmt.Errorf("failed to get or create queue for task %s: %w", taskIdentifier, err)
	}

	// 3. Try to push the task to the queue
	slog.Debug("Attempting to push task to queue", "identifier", taskIdentifier, "priority", task.GetPriority(), "retryCount", task.GetRetryCount())
	pushErr := taskQueue.Push(ctx, task)

	if pushErr == nil {
		// Increment queue count only on successful push
		atomic.AddInt64(&m.metrics.TasksQueued, 1)
		logMsg := "New task added successfully"
		if isRetry {
			logMsg = "Task re-enqueued successfully"
		}
		slog.Info(logMsg, "identifier", taskIdentifier, "priority", task.GetPriority(), "retryCount", task.GetRetryCount())
		return nil // Success
	}

	// Push failed
	slog.Error("Failed to push task to queue", "identifier", taskIdentifier, "priority", task.GetPriority(), "retryCount", task.GetRetryCount(), "error", pushErr)
	// If push failed, remove the task from the set (whether it was new or a retry)
	m.taskSetManager.Remove(taskIdentifier)
	slog.Warn("Removed task from set after failing to push to queue", "identifier", taskIdentifier)

	// If it was a retry attempt that failed to push, mark it as finally failed.
	if isRetry {
		atomic.AddInt64(&m.metrics.TasksFailed, 1)
		// TasksQueued was already decremented by the processing attempt, and push failed, so count is correct.
	}
	// For new tasks, failing to push means it never really entered the system processing flow.

	return fmt.Errorf("failed to push task %s to queue: %w", taskIdentifier, pushErr)
}

// GetMetrics returns a snapshot of the current metrics of the task manager
// Note: Returns a copy of the Metrics struct, read operations are concurrency-safe
func (m *Manager) GetMetrics() Metrics {
	// Since the Metrics fields are all int64, atomic operations ensure concurrency safety,
	// returning a copy directly is safe. If Metrics contains complex types, locking may be needed.
	return Metrics{
		TasksQueued:    atomic.LoadInt64(&m.metrics.TasksQueued),
		TasksProcessed: atomic.LoadInt64(&m.metrics.TasksProcessed),
		TasksFailed:    atomic.LoadInt64(&m.metrics.TasksFailed),
		// WorkersActive metric is now obtained from ConsumerPoolManager, needs to be cast to int64
		WorkersActive: int64(m.consumerPoolManager.GetCurrentConsumers()),
	}
}

// Register registers a Worker instance with the task manager
// After registration, the Worker can receive and process its declared task types
func (m *Manager) Register(worker Worker) {
	m.mu.Lock() // Write lock needed for modifying workers and workerMap
	defer m.mu.Unlock()

	// Add the Worker to the workers list
	m.workers = append(m.workers, worker)

	// Get the list of task types the Worker can handle
	// Use Background context, as getting types should not be cancelled or timed out
	ctx := context.Background()
	supportedTypes := worker.Types(ctx)
	slog.Info("Registering worker", "supportedTypes", supportedTypes)

	// Associate task types with Worker instances in workerMap
	for _, taskType := range supportedTypes {
		if _, ok := m.workerMap[taskType]; ok {
			// If a Worker is already registered for this type, log an error and skip
			// Do not allow a task type to be handled by multiple Workers (current design)
			slog.Error("Worker already registered for task type, skipping", "type", taskType)
			continue
		}
		m.workerMap[taskType] = worker
		slog.Info("Task type registered to worker", "type", taskType)
	}
}

// getWorkers is a helper method for ProducerManager to get the current list of registered workers.
func (m *Manager) getWorkers() []Worker {
	m.mu.RLock() // Read lock needed for reading the workers list
	defer m.mu.RUnlock()
	// Return a copy of the workers list to avoid concurrent modification issues
	workersCopy := make([]Worker, len(m.workers))
	copy(workersCopy, m.workers)
	return workersCopy
}
