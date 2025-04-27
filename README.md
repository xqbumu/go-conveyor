# go-conveyor

## Table of Contents

- [go-conveyor](#go-conveyor)
	- [Table of Contents](#table-of-contents)
	- [Introduction](#introduction)
	- [Features](#features)
	- [Installation](#installation)
	- [Usage](#usage)
		- [Example](#example)
	- [Contributing](#contributing)
	- [License](#license)

## Introduction

`go-conveyor` is a task management system written in Go, designed to simplify task production, scheduling, and execution. The core component is the `Manager`, which orchestrates the entire process. Key features include:

* **Consumer Pool Management**: Efficiently processes tasks concurrently using goroutine pools, supporting task prioritization.
* **Producer Management**: Produces tasks on a schedule by triggering the `Produce` method of registered workers.
* **Task Set Management**: Automatically deduplicates tasks to prevent redundant execution and handles periodic cleanup.

## Features

* **Task Production**: Producers trigger the `Produce` method of registered workers to generate tasks periodically.
* **Task Scheduling**: The `ConsumerPoolManager` distributes tasks to different channels based on priority, managing goroutine pools for concurrent execution.
* **Task Execution**: Consumer goroutines read tasks from priority channels and execute them by calling the `Consume` method of the corresponding worker.
* **Task Deduplication**: The `TaskSetManager` automatically prevents duplicate task execution by tracking tasks that are currently queued or being processed.
* **Task Retry**: Supports configurable retrying of tasks after execution failures, including handling panics based on configuration (`RetryOnPanic`).
* **Task Timeout**: Provides control over individual task execution timeouts.
* **Graceful Shutdown**: The `Manager` supports graceful stopping, ensuring all components and running tasks are properly shut down.
* **Metrics**: Provides basic metrics on tasks queued, processed, failed, and active workers.
* **Configuration**: Allows customization of consumer pool sizes, producer intervals, task timeouts, and retry behavior via `ManagerConfig` and options.

## Installation

Install the package using:

```bash
go get github.com/xqbumu/go-conveyor
```

## Usage

### Example

Here are some examples to get you started:

*   [Simple Worker Example](_examples/simple_worker_test.go)
*   [Full Worker Example](_examples/full_worker_test.go)
*   [Faulty Worker Example](_examples/faulty_worker_test.go)

## Contributing

We welcome issues and pull requests. Feel free to contribute to the project.

## License

This project is licensed under the MIT License.
