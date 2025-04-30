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
		"produceTimeout", m.config.ProduceTimeout, // Add Produce timeout to the log
		"retryOnPanic", m.config.RetryOnPanic)
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

// processTaskWithRetry encapsulates the task processing and retry logic
// This function is now a method of Manager and is passed as processTaskFunc to ConsumerPoolManager
func (m *Manager) processTaskWithRetry(ctx context.Context, task ITask) {
	maxRetries := 3                      // Maximum number of retries
	taskIdentifier := task.GetIdentify() // Get task identifier for logging and metrics update

	// Look up the corresponding Worker based on the task type
	m.mu.RLock() // Read lock needed for reading workerMap
	worker, ok := m.workerMap[task.GetType()]
	m.mu.RUnlock()

	if !ok {
		// If no corresponding Worker is found, log an error and mark the task as failed
		slog.Error("No worker found for task type", "type", task.GetType(), "taskID", task.GetID())
		atomic.AddInt64(&m.metrics.TasksFailed, 1)
		// Remove the task identifier from taskSetManager to indicate processing is complete (failed)
		m.taskSetManager.Remove(taskIdentifier)
		atomic.AddInt64(&m.metrics.TasksQueued, -1) // Task processing ended, decrease queued count
		return                                      // Process the next task
	}

	// Create a context for the current task to support timeout and cancellation
	// The task context inherits from the context passed in by ConsumerPoolManager
	taskCtx := ctx
	var cancel context.CancelFunc
	if task.GetTimeout() > 0 {
		// If the task has a timeout set, create a context with timeout.
		taskCtx, cancel = context.WithTimeout(ctx, task.GetTimeout())
	} else {
		// If the task has no timeout, create a cancellable context.
		taskCtx, cancel = context.WithCancel(ctx)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	// Execute the task processing with retry logic
	for i := range maxRetries {
		slog.Debug("Attempting task execution", "identifier", taskIdentifier, "attempt", i+1, "maxRetries", maxRetries)

		err := m.executeTaskConsume(taskCtx, worker, task) // Use the helper function to execute

		if err == nil {
			// Task succeeded
			atomic.AddInt64(&m.metrics.TasksProcessed, 1)
			slog.Debug("Task processed successfully", "identifier", taskIdentifier, "attempt", i+1)
			m.taskSetManager.Remove(taskIdentifier)
			atomic.AddInt64(&m.metrics.TasksQueued, -1)
			return // Success, exit the function
		}

		// Task failed (error or panic)
		slog.Error("Task execution failed",
			"taskID", task.GetID(),
			"type", task.GetType(),
			"error", err,
			"retryAttempt", i+1)

		// Check if it is a panic and retryOnPanic is false
		if _, isPanic := err.(PanicError); isPanic && !m.config.RetryOnPanic {
			slog.Error("Task panicked and retryOnPanic is false, marking as failed",
				"taskID", task.GetID(),
				"type", task.GetType(),
				"panicError", err) // Log the wrapped panic error
			atomic.AddInt64(&m.metrics.TasksFailed, 1)
			m.taskSetManager.Remove(taskIdentifier)
			atomic.AddInt64(&m.metrics.TasksQueued, -1)
			return // If configured not to retry on panic, do not retry
		}

		// Check if the maximum number of retries has been reached
		if i == maxRetries-1 {
			atomic.AddInt64(&m.metrics.TasksFailed, 1)
			slog.Error("Task failed after max retries", "identifier", taskIdentifier, "maxRetries", maxRetries)
			m.taskSetManager.Remove(taskIdentifier)
			atomic.AddInt64(&m.metrics.TasksQueued, -1)
			return // Failed after retries, exit the function
		}

		// Check if the task context has been cancelled before retrying
		select {
		case <-taskCtx.Done():
			slog.Warn("Task canceled or timed out during retry wait",
				"taskID", task.GetID(),
				"type", task.GetType,
				"error", taskCtx.Err())
			atomic.AddInt64(&m.metrics.TasksFailed, 1)
			m.taskSetManager.Remove(taskIdentifier)
			atomic.AddInt64(&m.metrics.TasksQueued, -1)
			return // Context cancelled, exit the function
		default:
			// Continue retrying
		}

		// Wait before retrying (exponential backoff)
		retryDelay := time.Second * time.Duration(i+1)
		slog.Info("Retrying task", "taskID", task.GetID(), "type", task.GetType, "delay", retryDelay)
		time.Sleep(retryDelay) // Block and wait
	}
}

// AddTask adds a new task to the task manager
// Parameters:
//   - ctx: Context to control the timeout or cancellation of the addition operation (mainly affects high-priority task addition and normal task blocking wait)
//   - id: Unique ID of the task
//   - taskType: Type of the task
//   - data: Data to be processed by the task
//   - priority: Task priority (0: normal, >0: high priority)
//   - timeout: Task execution timeout (0 means no timeout)
//
// Returns:
//   - error: Returns an error if the task already exists, adding a high-priority task times out, or the context is cancelled
func (m *Manager) AddTask(ctx context.Context, task ITask) error {
	taskIdentifier := task.GetIdentify()

	// 1. Check if the task already exists and try to add it to the taskSetManager
	// Use TaskSetManager to check if the task exists and add it
	added, err := m.taskSetManager.Add(taskIdentifier)
	if err != nil {
		slog.Error("Failed to add task to task set manager", "identifier", taskIdentifier, "error", err)
		return fmt.Errorf("failed to add task to task set manager: %s", taskIdentifier)
	}
	if !added {
		slog.Warn("Task already exists, skipping addition", "identifier", taskIdentifier)
		return fmt.Errorf("task already exists: %s", taskIdentifier)
	}
	atomic.AddInt64(&m.metrics.TasksQueued, 1) // Task successfully added to the set, increase queued count

	slog.Debug("Attempting to add task to queue", "identifier", taskIdentifier, "priority", task.GetPriority())

	// 3. Get or create the corresponding queue through ConsumerPoolManager
	// ConsumerPoolManager will be responsible for creating the queue and starting consumers
	taskQueue, err := m.consumerPoolManager.GetOrCreateQueue(task.GetPriority(), taskIdentifier, m.processTaskWithRetry)
	if err != nil {
		// GetOrCreateQueue failed, rollback state
		m.rollbackAddTask(taskIdentifier)
		slog.Error("Failed to get or create task queue", "identifier", taskIdentifier, "priority", task.GetPriority(), "error", err)
		return err
	}

	// 4. Try to push the task to the queue
	sendErr := taskQueue.Push(ctx, task)
	taskSent := sendErr == nil // Task is sent if Push returns no error

	if sendErr == nil {
		slog.Info("Task added successfully", "identifier", taskIdentifier, "priority", task.GetPriority())
	} else {
		slog.Error("Failed to push task to queue", "identifier", taskIdentifier, "priority", task.GetPriority(), "error", sendErr)
	}

	// If the task was not successfully sent, perform a rollback
	if !taskSent {
		m.rollbackAddTask(taskIdentifier)
	}

	// 5. Return the send result
	return sendErr
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
