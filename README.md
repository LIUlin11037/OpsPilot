# OpsPilot

OpsPilot 面向异构微服务的故障诊断。当前支持通过 CLI 提交「消息已入队但未消费」的模拟诊断任务。

在仓库根目录使用 Python 3.10 或更高版本运行以下命令（Windows PowerShell）。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m opspilot
.\.venv\Scripts\python.exe -m pytest
```

启动成功时输出 `OpsPilot is ready.`。安装依赖后，启动和测试均可离线运行。

## 模拟诊断

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario consumer-stopped "消息已入队但未消费"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario normal "消息已入队但未消费"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario queue-timeout "消息已入队但未消费"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario consumer-data-missing "消息已入队但未消费"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario processing-failed "消费者在运行，为什么队列还有积压？"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario processing-failed-outside-sample "消费者在运行，为什么队列还有积压？"
```

`--scenario` 选择本地模拟状态。LangGraph 先查询队列，发现待消费消息时再查询消费者。`normal` 场景只返回队列证据，`consumer-stopped` 场景返回队列和消费者两条证据。

`queue-timeout` 场景记录队列查询超时并结束调查。`consumer-data-missing` 场景保留已观测到的积压，并记录消费者数据缺失。两种场景的诊断状态均为 `undetermined`。

`processing-failed` 场景确认队列有积压且消费者正在运行后，再让目标适配器只读查询最近一次处理结果。报告保留队列的待消费消息 ID 样本和处理记录的消息 ID，积压原因仍标为 `undetermined`。

| `processing_correlation` | 含义 |
| --- | --- |
| `matched` | 处理记录的消息 ID 出现在队列样本中。 |
| `not_in_sample` | 处理记录的消息 ID 在样本之外，判断范围仅限样本。可用 `processing-failed-outside-sample` 复现。 |
| `unknown` | 处理查询已尝试，当前证据不足以进行 ID 比对。 |
| `null` | 调查在处理查询之前结束。 |

JSON 报告包含任务 ID、用户提交的问题、诊断状态和结论。每条证据记录工具、来源、UTC 观察时间及结果类型 `outcome`。结果为 `observed` 时附带观测值。模拟场景直接返回 `timeout` 或 `missing`，便于离线复现。

新报告的 `tool_calls` 按调用顺序记录工具名、结果类型和耗时 `duration_ms`。耗时用单调时钟测量，覆盖工具调用本身。模拟超时会立即返回 `timeout`，因此仍可能只有很短的耗时。早期保存的报告可按其原始内容读取。

## 回看诊断报告

`diagnose` 会先把报告保存到当前目录下的 `.opspilot/reports.sqlite3`，然后输出包含 `task_id` 的 JSON。用实际的任务 ID 替换示例中的 `TASK_ID`，即可读取同一份报告。

```powershell
.\.venv\Scripts\python.exe -m opspilot report "TASK_ID"
```

如需指定数据库路径，在子命令前加 `--db PATH`。诊断和查询时使用同一路径。

## 只读查询 RabbitMQ 队列

在已启用 Management API 的 RabbitMQ 环境中，将查询凭据提供给进程的 `OPSPILOT_RABBITMQ_USER` 和 `OPSPILOT_RABBITMQ_PASSWORD` 环境变量。以下命令使用本机地址和默认虚拟主机。

```powershell
.\.venv\Scripts\python.exe -m opspilot inspect-queue --management-url http://127.0.0.1:15672 --vhost "/" --queue "orders"
```

`inspect-queue` 将单个队列的 `messages_ready` 映射为 `pending_messages`，表示等待投递的消息数。查询只读取队列指标，因此消息 ID 样本为空。命令直接输出一条证据；超时、鉴权失败或缺少指标等情况会附带失败原因，并以状态码 1 退出。 [RabbitMQ 队列指标文档](https://www.rabbitmq.com/docs/monitoring#individual-queue-metrics)
