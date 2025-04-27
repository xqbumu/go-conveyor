# go-conveyor

## 目录

- [go-conveyor](#go-conveyor)
  - [目录](#目录)
  - [简介](#简介)
  - [功能](#功能)
  - [安装](#安装)
  - [使用方法](#使用方法)
    - [示例](#示例)
  - [贡献](#贡献)
  - [许可证](#许可证)

## 简介

`go-conveyor` 是一个用 Go 编写的任务管理系统，旨在简化任务的生产、调度和执行。主要功能包括：

* **消费者池管理**：使用 goroutine 池高效地并发处理任务。
* **生产者管理**：通过可自定义的逻辑按计划生成任务。
* **任务集合管理**：自动去重以防止重复执行任务。

## 功能

* **任务生产**：生产者触发工作者的 `Produce` 方法以生成任务。
* **任务调度**：消费者池管理器根据优先级将任务分配到不同的通道。
* **任务执行**：消费者 goroutine 从通道中读取并执行任务。
* **任务去重**：自动防止重复任务执行。
* **任务重试**：支持在任务执行失败后重试。
* **任务超时**：提供对任务执行超时的控制。

## 安装

使用以下命令安装该包：

```bash
go get github.com/xqbumu/go-conveyor
```

## 使用方法

### 示例

以下是一个演示如何使用 `go-conveyor` 的示例：

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/xqbumu/go-conveyor/task"
)

func main() {
	// 创建一个 TaskManager 实例
	tm := task.NewTaskManager()

	// 注册一个 Worker
	tm.RegisterWorker("example_worker", &ExampleWorker{})

	// 启动 TaskManager
	ctx := context.Background()
	tm.Start(ctx)

	// 添加一个任务
	tm.AddTask(ctx, "example_worker", task.Task{
		ID:   "1",
		Type: "example_task",
		Data: map[string]interface{}{
			"message": "Hello, world!",
		},
	})

	// 等待一段时间
	time.Sleep(5 * time.Second)

	// 停止 TaskManager
	tm.Stop()
}

type ExampleWorker struct{}

func (w *ExampleWorker) Produce(ctx context.Context) error {
	// 生成任务的逻辑
	return nil
}

func (w *ExampleWorker) Process(ctx context.Context, task task.Task) error {
	// 处理任务的逻辑
	fmt.Println(task.Data["message"])
	return nil
}
```

## 贡献

我们欢迎问题报告和拉取请求。欢迎为本项目做出贡献。

## 许可证

本项目基于 MIT 许可证授权。