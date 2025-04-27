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

`go-conveyor` is a task management system written in Go, designed to simplify task production, scheduling, and execution. Key features include:

* **Consumer Pool Management**: Efficiently processes tasks concurrently using goroutine pools.
* **Producer Management**: Produces tasks on a schedule with customizable logic.
* **Task Set Management**: Automatically deduplicates tasks to prevent redundant execution.

## Features

* **Task Production**: Producers trigger the `Produce` method of workers to generate tasks.
* **Task Scheduling**: The consumer pool manager distributes tasks to different channels based on priority.
* **Task Execution**: Consumer goroutines read and execute tasks from channels.
* **Task Deduplication**: Automatically prevents duplicate task execution.
* **Task Retry**: Supports retrying tasks after execution failures.
* **Task Timeout**: Provides control over task execution timeouts.

## Installation

Install the package using:

```bash
go get github.com/xqbumu/go-conveyor
```

## Usage

### Example

Below is an example demonstrating how to use `go-conveyor`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/xqbumu/go-conveyor"
)

func main() {
	// Create a TaskManager instance
	tm := conveyor.NewTaskManager()

	// Register a Worker
	tm.RegisterWorker("example_worker", &ExampleWorker{})

	// Start the TaskManager
	ctx := context.Background()
	tm.Start(ctx)

	// Add a task
	tm.AddTask(ctx, "example_worker", conveyor.Task{
		ID:   "1",
		Type: "example_task",
		Data: map[string]interface{}{
			"message": "Hello, world!",
		},
	})

	// Wait for a while
	time.Sleep(5 * time.Second)

	// Stop the TaskManager
	tm.Stop()
}

type ExampleWorker struct{}

func (w *ExampleWorker) Produce(ctx context.Context) error {
	// Logic for producing tasks
	return nil
}

func (w *ExampleWorker) Process(ctx context.Context, task conveyor.Task) error {
	// Logic for processing tasks
	fmt.Println(conveyor.Data["message"])
	return nil
}
```

## Contributing

We welcome issues and pull requests. Feel free to contribute to the project.

## License

This project is licensed under the MIT License.
