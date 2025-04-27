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
	ticker         *time.Ticker    // Timer for triggering the Produce method of Workers
	getWorkers     GetWorkersFunc  // Function for obtaining the list of registered Workers
	wg             sync.WaitGroup  // WaitGroup for waiting for the producer goroutine to finish
	interval       time.Duration   // Interval for triggering the Produce method of Workers
	produceTimeout time.Duration   // Timeout for the Produce method of Workers
}

// NewProducerManager creates and initializes a new ProducerManager instance.
func NewProducerManager(ctx context.Context, interval time.Duration, produceTimeout time.Duration, getWorkers GetWorkersFunc) *ProducerManager {
	if getWorkers == nil {
		slog.Error("NewProducerManager requires a non-nil GetWorkersFunc")
		// In a real application, you might want to return an error or panic
		return nil
	}
	pm := &ProducerManager{
		ctx:            ctx,            // Store context
		getWorkers:     getWorkers,     // Store function to get Workers
		interval:       interval,       // Store interval
		produceTimeout: produceTimeout, // Store Produce timeout
	}
	slog.Info("ProducerManager created with interval and produce timeout", "interval", interval, "produceTimeout", produceTimeout)
	return pm
}

// Start starts the producer timer goroutine.
// This method will block until the provided context ctx is canceled.
func (pm *ProducerManager) Start() {
	slog.Info("Starting producer ticker", "interval", pm.interval)

	pm.ticker = time.NewTicker(pm.interval)

	pm.wg.Add(1) // Increment WaitGroup counter
	go func() {
		defer pm.wg.Done() // Notify WaitGroup when the producer goroutine exits
		defer slog.Info("Producer ticker stopped")
		for {
			select {
			case <-pm.ticker.C: // Timer triggered
				// Get the list of registered Workers
				workers := pm.getWorkers()

				// Iterate and call the Produce method of each Worker
				for _, worker := range workers {
					// Create a separate context with timeout for the Produce method
					produceCtx, produceCancel := context.WithTimeout(pm.ctx, pm.produceTimeout)

					if err := worker.Produce(produceCtx); err != nil {
						// Log production error, but do not interrupt the loop
						slog.Error("Producer error", "error", err)
					}
					produceCancel() // Cancel the context for the Produce method
				}
			case <-pm.ctx.Done(): // External context canceled
				return // Exit goroutine
			}
		}
	}()
}

// Stop stops the producer timer and waits for the producer goroutine to finish.
func (pm *ProducerManager) Stop() {
	slog.Info("Stopping ProducerManager components...")
	// Stop the producer timer
	if pm.ticker != nil {
		pm.ticker.Stop()
		slog.Info("Producer ticker stopped")
	}

	// Wait for the producer goroutine to finish
	slog.Info("Waiting for producer goroutine to finish...")
	pm.wg.Wait()
	slog.Info("Producer goroutine finished.")
}
