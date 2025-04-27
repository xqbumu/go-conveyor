package conveyor

// Metrics represents task manager metrics used to monitor task processing status.
type Metrics struct {
	TasksQueued    int64 // The number of tasks currently waiting in the queue to be processed
	TasksProcessed int64 // The total number of tasks that have been successfully processed
	TasksFailed    int64 // The total number of tasks that failed to process (including retry failures)
	WorkersActive  int64 // The number of workers (goroutines) currently actively processing tasks
}

// GetMetrics returns a snapshot of the current task manager metrics
// Note: The returned value is a copy of the Metrics struct, and the read operation is thread-safe
// This method was originally in Manager, but since the Metrics struct has been moved here,
// and GetMetrics simply reads atomic variables, it can be considered as a method of Metrics,
// or kept in Manager but requires importing the metrics package.
// To maintain the core logic of Manager, GetMetrics is temporarily kept in Manager,
// only the Metrics struct is moved here.
