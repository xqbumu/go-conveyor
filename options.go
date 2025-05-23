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

// ManagerConfig contains configuration options for the TaskManager.
type ManagerConfig struct {
	ConsumerPoolManagerConfig
	ProducerCronSchedule string           // Cron schedule string for the producer to trigger the Produce method
	ProduceTimeout       time.Duration    // Timeout for the producer's Produce method (0 means no timeout)
	RetryOnPanic         bool             // Whether to retry tasks when a Worker panics
	MaxRetries           int              // Maximum number of times a task should be retried after failure (0 means no retries)
	TaskQueueFactory     TaskQueueFactory // Factory function to create TaskQueue instances
}

// WithTaskQueueFactory configures the factory function to create TaskQueue instances.
func WithTaskQueueFactory(factory TaskQueueFactory) Option {
	return func(cfg *ManagerConfig) {
		cfg.TaskQueueFactory = factory
	}
}

// WithRetryOnPanic configures whether to retry tasks when a Worker panics.
func WithRetryOnPanic(retry bool) Option {
	return func(cfg *ManagerConfig) {
		cfg.RetryOnPanic = retry
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

// WithMaxRetries configures the maximum number of times a task should be retried.
func WithMaxRetries(max int) Option {
	return func(cfg *ManagerConfig) {
		if max < 0 {
			max = 0 // Ensure max retries is not negative
		}
		cfg.MaxRetries = max
	}
}
