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

Here are some examples to get you started:

*   [Simple Worker Example](_examples/simple_worker_test.go)
*   [README Worker Example](_examples/readme_worker_test.go)
*   [Faulty Worker Example](_examples/faulty_worker_test.go)

## Contributing

We welcome issues and pull requests. Feel free to contribute to the project.

## License

This project is licensed under the MIT License.
