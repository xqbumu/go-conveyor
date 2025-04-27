package conveyor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3" // Import the cron library
)

// GetWorkersFunc defines a function type for obtaining the current list of registered Workers.
type GetWorkersFunc func() []Worker

// ProducerManager is responsible for managing producer cron jobs and triggering the Produce method of Workers.
type ProducerManager struct {
	ctx  context.Context // Context for managing cancellation and timeouts
	cron *cron.Cron      // Cron scheduler instance
	wg   sync.WaitGroup  // WaitGroup for waiting for all producer goroutines to finish
	mu   sync.Mutex      // Mutex to protect maps and cron instance

	getWorkers     GetWorkersFunc // Function for obtaining the list of registered Workers
	cronSchedule   string         // Global cron schedule string for triggering the Produce method of Workers
	produceTimeout time.Duration  // Timeout for the Produce method of Workers

	workerEntryIDs  map[Worker]cron.EntryID       // Map to store cron entry IDs for each worker
	workerCancels   map[Worker]context.CancelFunc // Map to store cancellation functions for each worker's context
	workerSchedules map[Worker]string             // Map to store the configured cron schedule for each worker
}

// NewProducerManager creates and initializes a new ProducerManager instance.
func NewProducerManager(ctx context.Context, cronSchedule string, produceTimeout time.Duration, getWorkers GetWorkersFunc) *ProducerManager {
	if getWorkers == nil {
		slog.Error("NewProducerManager requires a non-nil GetWorkersFunc")
		return nil
	}
	pm := &ProducerManager{
		ctx:  ctx,                          // Store context
		cron: cron.New(cron.WithSeconds()), // Initialize the cron scheduler

		getWorkers:     getWorkers,     // Store function to get Workers
		cronSchedule:   cronSchedule,   // Store global cron schedule
		produceTimeout: produceTimeout, // Store Produce timeout

		workerEntryIDs:  make(map[Worker]cron.EntryID),       // Initialize map
		workerCancels:   make(map[Worker]context.CancelFunc), // Initialize map
		workerSchedules: make(map[Worker]string),             // Initialize map
	}

	slog.Info("ProducerManager created with global cron schedule and produce timeout", "global_cron_schedule", cronSchedule, "produceTimeout", produceTimeout)
	return pm
}

// Start starts the ProducerManager, including the cron scheduler.
func (pm *ProducerManager) Start() {
	slog.Info("Starting ProducerManager...")

	// Start the cron scheduler
	pm.cron.Start()

	// Initial population of workers
	pm.UpdateWorkers()
}

// UpdateWorkers updates the set of managed workers and their cron jobs.
// This method can be called dynamically to add, remove, or update workers.
func (pm *ProducerManager) UpdateWorkers() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	slog.Info("Updating workers in ProducerManager...")

	// Get the current list of workers from the provided function
	newWorkers := pm.getWorkers()

	// Create a map of the new workers for easy lookup and to store their desired schedules
	newWorkersMap := make(map[Worker]string)
	for _, worker := range newWorkers {
		schedule := pm.cronSchedule // Use global schedule if worker schedule is not set or invalid
		if worker, ok := worker.(WorkerProduceCronSchedule); ok {
			if workerSchedule, ok := worker.ProduceCronSchedule(); ok && workerSchedule != "" {
				schedule = workerSchedule
			}
		}
		newWorkersMap[worker] = schedule
	}

	// Identify workers to remove or update
	for worker, entryID := range pm.workerEntryIDs {
		newSchedule, exists := newWorkersMap[worker]
		if !exists {
			// Worker removed
			slog.Info("Removing worker from ProducerManager", "worker", worker)
			pm.cron.Remove(entryID) // Remove cron job
			if cancel, ok := pm.workerCancels[worker]; ok {
				cancel() // Cancel the worker's context
			}
			delete(pm.workerEntryIDs, worker)
			delete(pm.workerCancels, worker)
			delete(pm.workerSchedules, worker) // Remove from schedule map
		} else {
			// Worker exists, check if schedule changed
			currentSchedule := pm.workerSchedules[worker]
			if newSchedule != currentSchedule {
				// Schedule changed, remove old cron job and add a new one
				slog.Info("Worker schedule changed, updating cron job", "worker", worker, "old_schedule", currentSchedule, "new_schedule", newSchedule)
				pm.cron.Remove(entryID) // Remove old cron job
				if cancel, ok := pm.workerCancels[worker]; ok {
					cancel() // Cancel the old context
				}
				delete(pm.workerEntryIDs, worker)
				delete(pm.workerCancels, worker)
				delete(pm.workerSchedules, worker) // Remove old schedule
				// The new cron job will be added below
			} else {
				// Schedule did not change, keep the existing cron job
				slog.Debug("Worker schedule unchanged, keeping existing cron job", "worker", worker, "schedule", currentSchedule)
				// Remove from newWorkersMap so we don't try to add it again
				delete(newWorkersMap, worker)
			}
		}
	}

	// Identify workers to add or restart (due to potential schedule change)
	for worker, schedule := range newWorkersMap {
		slog.Info("Adding or restarting worker in ProducerManager", "worker", worker, "schedule", schedule)

		// Create a new context for this worker's cron job execution
		workerCtx, workerCancel := context.WithCancel(pm.ctx)
		pm.workerCancels[worker] = workerCancel
		pm.workerSchedules[worker] = schedule // Store the configured schedule

		// Add the cron job
		entryID, err := pm.cron.AddFunc(schedule, func() {
			// This function is executed by the cron scheduler
			pm.wg.Add(1)       // Increment WaitGroup counter for this cron job execution
			defer pm.wg.Done() // Notify WaitGroup when this execution finishes

			slog.Debug("Cron job triggered for worker", "worker", worker, "schedule", schedule)
			pm.callProduce(workerCtx, worker) // Call the Produce method
		})
		if err != nil {
			slog.Error("Failed to add cron job for worker", "worker", worker, "schedule", schedule, "error", err)
			// Clean up the context if cron job addition failed
			workerCancel()
			delete(pm.workerCancels, worker)
			delete(pm.workerSchedules, worker)
			continue // Skip to the next worker
		}
		pm.workerEntryIDs[worker] = entryID // Store the new entry ID
	}
	slog.Info("Finished updating workers in ProducerManager.")
}

// callProduce calls the Produce method of a worker with a timeout context.
func (pm *ProducerManager) callProduce(ctx context.Context, worker Worker) {
	// Create a separate context with timeout for the Produce method
	produceCtx, produceCancel := context.WithTimeout(ctx, pm.produceTimeout)
	defer produceCancel() // Ensure context is canceled

	slog.Debug("Calling Produce for worker", "worker", worker)
	if err := worker.Produce(produceCtx); err != nil {
		// Log production error, but do not interrupt the cron schedule
		slog.Error("Producer error", "worker", worker, "error", err)
	}
}

// Stop stops the ProducerManager, including the cron scheduler, and waits for all running jobs to finish.
func (pm *ProducerManager) Stop() {
	slog.Info("Stopping ProducerManager components...")

	// Stop the cron scheduler gracefully
	// This stops the scheduler and waits for currently running jobs to complete
	slog.Info("Stopping cron scheduler...")
	stopCtx := pm.cron.Stop()
	slog.Info("Waiting for cron scheduler to stop...")
	<-stopCtx.Done()
	slog.Info("Cron scheduler stopped.")

	// Cancel all worker contexts to signal running jobs to stop if they are using the context
	pm.mu.Lock()
	for worker, cancel := range pm.workerCancels {
		cancel()
		slog.Info("Worker context canceled during stop", "worker", worker)
	}
	pm.mu.Unlock()

	// Wait for any remaining Produce calls triggered by cron before Stop was called
	slog.Info("Waiting for any remaining producer goroutines to finish...")
	pm.wg.Wait()
	slog.Info("All producer goroutines finished.")

	slog.Info("ProducerManager stopped gracefully.")
}
