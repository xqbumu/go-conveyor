package conveyor

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// GetWorkersFunc defines a function type for obtaining the current list of registered Workers.
type GetWorkersFunc func() []Worker

// ProducerManager is responsible for managing producer timers and triggering the Produce method of Workers.
type ProducerManager struct {
	ctx            context.Context // Context for managing cancellation and timeouts
	getWorkers     GetWorkersFunc  // Function for obtaining the list of registered Workers
	wg             sync.WaitGroup  // WaitGroup for waiting for all producer goroutines to finish
	interval       time.Duration   // Global interval for triggering the Produce method of Workers
	produceTimeout time.Duration   // Timeout for the Produce method of Workers

	workerTickers   map[Worker]*time.Ticker       // Map to store tickers for each worker
	workerCancels   map[Worker]context.CancelFunc // Map to store cancellation functions for each worker's goroutine
	workerIntervals map[Worker]time.Duration      // Map to store the configured interval for each worker
	mu              sync.Mutex                    // Mutex to protect maps
}

// NewProducerManager creates and initializes a new ProducerManager instance.
func NewProducerManager(ctx context.Context, interval time.Duration, produceTimeout time.Duration, getWorkers GetWorkersFunc) *ProducerManager {
	if getWorkers == nil {
		slog.Error("NewProducerManager requires a non-nil GetWorkersFunc")
		// In a real application, you might want to return an error or panic
		return nil
	}
	pm := &ProducerManager{
		ctx:             ctx,                                 // Store context
		getWorkers:      getWorkers,                          // Store function to get Workers
		interval:        interval,                            // Store global interval
		produceTimeout:  produceTimeout,                      // Store Produce timeout
		workerTickers:   make(map[Worker]*time.Ticker),       // Initialize map
		workerCancels:   make(map[Worker]context.CancelFunc), // Initialize map
		workerIntervals: make(map[Worker]time.Duration),      // Initialize map
	}
	slog.Info("ProducerManager created with global interval and produce timeout", "global_interval", interval, "produceTimeout", produceTimeout)
	return pm
}

// Start starts the producer goroutines for each worker.
// This method will block until the provided context ctx is canceled.
func (pm *ProducerManager) Start() {
	slog.Info("Starting ProducerManager...")

	// Initial population of workers
	pm.UpdateWorkers(pm.getWorkers())
}

// UpdateWorkers updates the set of managed workers and their tickers.
// This method can be called dynamically to add, remove, or update workers.
func (pm *ProducerManager) UpdateWorkers(newWorkers []Worker) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	slog.Info("Updating workers in ProducerManager...")

	// Create a map of the new workers for easy lookup and to store their desired intervals
	newWorkersMap := make(map[Worker]time.Duration)
	for _, worker := range newWorkers {
		interval := pm.interval // Use global interval if worker interval is not set or invalid
		if worker, ok := worker.(WorkerProduceInterval); ok {
			if workerInterval, ok := worker.ProduceInterval(); ok && workerInterval > 0 {
				interval = workerInterval
			}
		}
		newWorkersMap[worker] = interval
	}

	// Identify workers to remove or update
	for worker, cancel := range pm.workerCancels {
		newInterval, exists := newWorkersMap[worker]
		if !exists {
			// Worker removed
			slog.Info("Removing worker from ProducerManager", "worker", worker)
			pm.workerTickers[worker].Stop()
			cancel() // Cancel the worker's context
			delete(pm.workerTickers, worker)
			delete(pm.workerCancels, worker)
			delete(pm.workerIntervals, worker) // Remove from interval map
			// Decrement WaitGroup counter for removed worker's goroutine
			// Note: The goroutine will exit after context cancellation and call Done()
		} else {
			// Worker exists, check if interval changed
			currentInterval := pm.workerIntervals[worker]
			if newInterval != currentInterval {
				// Interval changed, restart the worker's goroutine
				slog.Info("Worker interval changed, restarting goroutine", "worker", worker, "old_interval", currentInterval, "new_interval", newInterval)
				pm.workerTickers[worker].Stop()
				cancel() // Cancel the old context
				delete(pm.workerTickers, worker)
				delete(pm.workerCancels, worker)
				delete(pm.workerIntervals, worker) // Remove old interval
				// The goroutine will exit and call Done(). We will add a new one below.
			} else {
				// Interval did not change, keep the existing goroutine and ticker
				slog.Debug("Worker interval unchanged, keeping existing goroutine", "worker", worker, "interval", currentInterval)
				// Remove from newWorkersMap so we don't try to add it again
				delete(newWorkersMap, worker)
			}
		}
	}

	// Identify workers to add or restart (due to potential interval change)
	for worker, interval := range newWorkersMap {
		slog.Info("Adding or restarting worker in ProducerManager", "worker", worker, "interval", interval)

		// Create a new context for this worker's goroutine
		workerCtx, workerCancel := context.WithCancel(pm.ctx)
		pm.workerCancels[worker] = workerCancel
		pm.workerIntervals[worker] = interval // Store the configured interval

		// Create a new ticker for this worker
		ticker := time.NewTicker(interval)
		pm.workerTickers[worker] = ticker

		pm.wg.Add(1) // Increment WaitGroup counter for this worker's new goroutine
		go func(w Worker, t *time.Ticker, c context.Context) {
			defer pm.wg.Done() // Notify WaitGroup when this worker's goroutine exits
			defer slog.Info("Worker producer goroutine stopped", "worker", w)
			defer t.Stop() // Stop the ticker when the goroutine exits

			slog.Info("Worker producer goroutine started", "worker", w)

			// Immediately trigger Produce once when starting/restarting
			pm.callProduce(c, w)

			for {
				select {
				case <-t.C: // Timer triggered for this worker
					pm.callProduce(c, w)
				case <-c.Done(): // Context canceled for this worker
					return // Exit goroutine
				}
			}
		}(worker, ticker, workerCtx) // Pass worker, ticker, and context to the goroutine
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
		// Log production error, but do not interrupt the loop
		slog.Error("Producer error", "worker", worker, "error", err)
	}
}

// Stop stops all worker producer timers and waits for their goroutines to finish.
func (pm *ProducerManager) Stop() {
	slog.Info("Stopping ProducerManager components...")

	pm.mu.Lock()
	// Stop all worker tickers and cancel their contexts
	for worker, ticker := range pm.workerTickers {
		ticker.Stop()
		slog.Info("Worker ticker stopped", "worker", worker)
		if cancel, ok := pm.workerCancels[worker]; ok {
			cancel()
			slog.Info("Worker context canceled", "worker", worker)
		}
	}
	pm.mu.Unlock()

	// Wait for all worker goroutines to finish
	slog.Info("Waiting for all producer goroutines to finish...")
	pm.wg.Wait()
	slog.Info("All producer goroutines finished.")
}
