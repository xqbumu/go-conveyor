# Changelog

## [v0.0.5] - 2025-04-30

### Fixes

- Fixed `ineffassign` linting error in `task_queue_socket_test.go`.
- Removed deprecated `rand.Seed` call in `task_queue_socket.go` (SA1019).

### Refactor

- Remove unused rollbackAddTask helper function from Manager.
- Introduce retry mechanism for task processing with configurable max retries.
- Enhance cancellation handling in ConsumerPoolManager and TaskQueue methods.
- Update Push method to accept context and enhance TaskQueue interface.
- Introduce SocketQueueOptions for configurable SocketTaskQueue and update related methods.
- Define magic number as a byte slice and simplify its usage in message handling.
- Simplify error handling and improve message reading/writing in SocketTaskQueue.

### Test

- Add TestInvalidJSONHandling to verify server behavior with invalid TaskData JSON.

### 修复

- 修复了 `task_queue_socket_test.go` 中的 `ineffassign` linting 错误。
- 移除了 `task_queue_socket.go` 中已弃用的 `rand.Seed` 调用 (SA1019)。

### 重构

- 从 Manager 中移除未使用的 rollbackAddTask 辅助函数。
- 引入任务处理重试机制，支持可配置的最大重试次数。
- 增强 ConsumerPoolManager 和 TaskQueue 方法中的取消处理。
- 更新 Push 方法以接受 context 并增强 TaskQueue 接口。
- 引入 SocketQueueOptions 以支持可配置的 SocketTaskQueue 并更新相关方法。
- 将魔术数字定义为字节切片并简化其在消息处理中的使用。
- 简化 SocketTaskQueue 中的错误处理并改进消息读写。

### 测试

- 添加 TestInvalidJSONHandling 以验证服务器在处理无效 TaskData JSON 时的行为。



## [v0.0.4] - 2025-04-29

### Features

- Implement TaskQueueChannel using Go channels for task management.

### 功能

- 使用 Go channel 实现 TaskQueueChannel 以进行任务管理。

## [v0.0.3] - 2025-04-29

### Features

- Enhance ConsumerPoolManager to utilize TaskQueueFactory for dynamic task queue creation.

### 功能

- 增强 ConsumerPoolManager 以利用 TaskQueueFactory 动态创建任务队列。

## [v0.0.2] - 2025-04-28

### Features

- Refactor task handling to use ITask interface and enhance worker implementations.
- Update Manager to use configurable TaskQueueFactory and remove TaskQueueType.
- Implement TaskQueue interface with ChannelTaskQueue and update ConsumerPoolManager to use it.

### 功能

- 重构任务处理以使用 ITask 接口并增强工作者实现。
- 更新 Manager 以使用可配置的 TaskQueueFactory 并移除 TaskQueueType。
- 实现 TaskQueue 接口与 ChannelTaskQueue 并更新 ConsumerPoolManager 以使用它。

## [v0.0.1] - 2025-04-27

### Features

- Implement task management system with worker and consumer pool.
- Add .gitignore file to exclude build artifacts and IDE files.
- Add MIT License file to the repository.
- Enhance ProducerManager to support dynamic worker intervals and improve logging.
- Update examples in README files to reflect new full worker implementation.
- Enhance README files with detailed descriptions of core components and features.
- Refactor ProducerManager to use cron schedules for task production.

### Documentation

- Add examples section to README files for better user guidance.

### Refactor

- Update task management references in README files.
- Remove example code from README files and improve context management in task manager.

### 功能

- 实现带有 worker 和 consumer pool 的任务管理系统。
- 添加 .gitignore 文件以排除构建产物和 IDE 文件。
- 添加 MIT License 文件到仓库。
- 增强 ProducerManager 以支持动态 worker 间隔并改进日志记录。
- 更新 README 文件中的示例以反映新的完整 worker 实现。
- 增强 README 文件，详细描述核心组件和功能。
- 重构 ProducerManager 以使用 cron 调度进行任务生产。

### 文档

- 在 README 文件中添加示例部分以提供更好的用户指导。

### 重构

- 更新 README 文件中的任务管理引用。
- 从 README 文件中移除示例代码并改进任务管理器中的上下文管理。
