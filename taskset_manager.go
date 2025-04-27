package conveyor

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// TaskSetManager is responsible for managing the set of added task identifiers, used for task deduplication and periodic cleanup.
type TaskSetManager struct {
	taskSet       map[string]struct{} // Records the added task identifiers (ID:Type)
	mu            sync.RWMutex        // Read-write lock to protect concurrent access to taskSet
	cleanupTicker *time.Ticker        // Timer for periodic cleanup of taskSet
}

// NewTaskSetManager creates and initializes a new TaskSetManager instance.
func NewTaskSetManager() *TaskSetManager {
	tsm := &TaskSetManager{
		taskSet: make(map[string]struct{}),
	}
	// Start periodic cleanup task, executed every 30 minutes
	tsm.cleanupTicker = time.NewTicker(30 * time.Minute)
	go tsm.cleanup()
	slog.Info("TaskSetManager created and cleanup routine started")
	return tsm
}

// Add attempts to add a task identifier to the task set.
// If the task identifier already exists, it returns false and an error.
// If the task identifier does not exist, it adds it and returns true and nil.
func (tsm *TaskSetManager) Add(taskIdentifier string) (bool, error) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	if _, exists := tsm.taskSet[taskIdentifier]; exists {
		slog.Warn("Task already exists in set", "identifier", taskIdentifier)
		return false, fmt.Errorf("task already exists: %s", taskIdentifier)
	}
	tsm.taskSet[taskIdentifier] = struct{}{}
	slog.Debug("Task added to set", "identifier", taskIdentifier)
	return true, nil
}

// Remove removes a task identifier from the task set.
func (tsm *TaskSetManager) Remove(taskIdentifier string) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()
	delete(tsm.taskSet, taskIdentifier)
	slog.Debug("Task removed from set", "identifier", taskIdentifier)
}

// Exists checks if a task identifier already exists in the task set.
func (tsm *TaskSetManager) Exists(taskIdentifier string) bool {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()
	_, exists := tsm.taskSet[taskIdentifier]
	return exists
}

// cleanup periodically cleans up the processed task ID set (taskSet) to prevent unlimited memory growth.
// The current implementation retains the most recent (not precise) 1000 task IDs.
func (tsm *TaskSetManager) cleanup() {
	for range tsm.cleanupTicker.C { // Wait for the timer to trigger
		tsm.mu.Lock() // Lock to protect concurrent access to taskSet
		if len(tsm.taskSet) > 1000 {
			slog.Info("TaskSetManager cleanup triggered", "currentSize", len(tsm.taskSet))
			newSet := make(map[string]struct{}, 1000) // Create a new map
			i := 0
			// Iterate over the old taskSet, randomly retaining up to 1000 elements in the new set
			// Note: This method does not guarantee that the most recent task IDs are retained
			for k := range tsm.taskSet {
				if i >= 1000 {
					break
				}
				newSet[k] = struct{}{}
				i++
			}
			tsm.taskSet = newSet // Replace the old taskSet
			slog.Info("TaskSetManager taskSet cleaned up", "newSize", len(tsm.taskSet))
		}
		tsm.mu.Unlock()
	}
}

// Stop stops the cleanup timer of the TaskSetManager.
func (tsm *TaskSetManager) Stop() {
	if tsm.cleanupTicker != nil {
		tsm.cleanupTicker.Stop()
		slog.Info("TaskSetManager cleanup ticker stopped")
	}
}
