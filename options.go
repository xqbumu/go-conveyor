package conveyor

import (
	"time"
)

type ConsumerPoolManagerConfig struct {
	MaxPriorityChannels int32       // Maximum number of priority channels that can be dynamically created
	MaxTotalConsumers   int32       // Maximum total number of consumer goroutines allowed
	DefaultConsumers    int         // Default number of consumer goroutines
	DefaultBufferSize   int         // Default task channel buffer size
	PriorityConsumers   map[int]int // Configuration of the number of consumers for specific priority task channels
}

func NewConsumerPoolManagerConfig() ConsumerPoolManagerConfig {
	return ConsumerPoolManagerConfig{
		MaxPriorityChannels: 10,
		MaxTotalConsumers:   10,
		DefaultConsumers:    1,
		DefaultBufferSize:   10,
		PriorityConsumers:   make(map[int]int),
	}
}

// TaskQueueType defines the type of task queue to use.
type TaskQueueType string

const (
	TaskQueueTypeChannel TaskQueueType = "channel"
	// TODO: Add other task queue types like "redis", "database"
)

// ManagerConfig contains configuration options for the TaskManager.
type ManagerConfig struct {
	ConsumerPoolManagerConfig
	ProducerCronSchedule string        // Cron schedule string for the producer to trigger the Produce method
	ProduceTimeout       time.Duration // Timeout for the producer's Produce method (0 means no timeout)
	RetryOnPanic         bool          // Whether to retry tasks when a Worker panics
	TaskQueueType        TaskQueueType // Type of task queue to use (e.g., "channel", "redis", "database")
}

// NewManagerConfig creates a ManagerConfig instance with default configuration.
// This function is not strictly necessary with the Option pattern, but can be useful for clarity.
// func NewManagerConfig() ManagerConfig {
// 	return ManagerConfig{
// 		ConsumerPoolManagerConfig: NewConsumerPoolManagerConfig(),
// 		ProducerCronSchedule:      "",
// 		ProduceTimeout:            time.Minute,
// 		RetryOnPanic:              false,
// 		TaskQueueType:             TaskQueueTypeChannel, // Default to channel
// 	}
// }

// WithRetryOnPanic configures whether to retry tasks when a Worker panics.
func WithRetryOnPanic(retry bool) Option {
	return func(cfg *ManagerConfig) {
		cfg.RetryOnPanic = retry
	}
}

// WithTaskQueueType configures the type of task queue to use.
func WithTaskQueueType(queueType TaskQueueType) Option {
	return func(cfg *ManagerConfig) {
		cfg.TaskQueueType = queueType
	}
}

// Option defines a function type for configuring the TaskManager.
// The Option function now takes a ManagerConfig pointer to modify the configuration.
type Option func(*ManagerConfig)

// WithMaxPriorityChannels configures the maximum number of priority channels that can be dynamically created.
func WithMaxPriorityChannels(max int32) Option {
	return func(cfg *ManagerConfig) {
		cfg.MaxPriorityChannels = max
	}
}

// WithMaxTotalConsumers configures the maximum total number of consumer goroutines allowed.
func WithMaxTotalConsumers(max int32) Option {
	return func(cfg *ManagerConfig) {
		cfg.MaxTotalConsumers = max
	}
}

// WithDefaultConsumers configures the default number of consumer goroutines.
func WithDefaultConsumers(count int) Option {
	return func(cfg *ManagerConfig) {
		cfg.DefaultConsumers = count
	}
}

// WithDefaultBufferSize configures the default task channel buffer size.
func WithDefaultBufferSize(size int) Option {
	return func(cfg *ManagerConfig) {
		cfg.DefaultBufferSize = size
	}
}

// WithPriorityConsumers configures the number of consumers for specific priority task channels.
func WithPriorityConsumers(config map[int]int) Option {
	return func(cfg *ManagerConfig) {
		// Copy the map to avoid external modifications affecting the internal configuration
		if cfg.PriorityConsumers == nil {
			cfg.PriorityConsumers = make(map[int]int)
		}
		for p, count := range config {
			cfg.PriorityConsumers[p] = count
		}
	}
}

// WithProducerCronSchedule configures the cron schedule string for the producer to trigger the Produce method.
func WithProducerCronSchedule(schedule string) Option {
	return func(cfg *ManagerConfig) {
		cfg.ProducerCronSchedule = schedule
	}
}

// WithProduceTimeout configures the timeout for the producer's Produce method.
func WithProduceTimeout(timeout time.Duration) Option {
	return func(cfg *ManagerConfig) {
		cfg.ProduceTimeout = timeout
	}
}
