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

`inspect-queue` 从同一次响应读取三个指标。`messages_ready` 映射为待投递数 `pending_messages`，`messages_unacknowledged` 映射为已投递待确认数 `unacknowledged_messages`，`consumers` 映射为消费者注册数 `registered_consumers`。缺失或无效的待确认数及注册数记为 `null`，有效的零值保留为 `0`。查询只读取队列指标，消息 ID 样本为空。待投递数不可用时，命令输出失败原因并以状态码 1 退出，同时保留其他有效指标。 [RabbitMQ 队列指标文档](https://www.rabbitmq.com/docs/monitoring#individual-queue-metrics) · [消费者指标定义](https://www.rabbitmq.com/docs/man/rabbitmqctl.8#consumers)

将相同的连接参数传给 `diagnose`，可以把这条队列观测纳入诊断报告并保存任务 ID。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose --management-url http://127.0.0.1:15672 --queue "orders" "这个队列有积压吗？"
```

RabbitMQ 模式保留队列证据及读取耗时。报告分别描述待投递数和已投递待确认数。待投递数为零时，`no_backlog_observed` 仅表示当次未观测到等待投递的消息，报告仍会列出待确认数。发现待投递积压时，报告也会说明消费者注册数，根因标为 `undetermined`。

### 结合业务处理记录

目标系统可以将最近一次处理尝试导出为 UTF-8 JSON。将下例保存为 `processing.json`，并用实际的队列、消息 ID、结果和时间替换示例值。`result` 支持 `succeeded` 和 `failed`，`attempted_at` 使用带时区的 ISO 8601 时间。

```json
{
  "vhost": "/",
  "queue": "orders",
  "message_id": "order-msg-101",
  "result": "failed",
  "attempted_at": "2026-11-19T03:00:00Z"
}
```

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose --management-url http://127.0.0.1:15672 --queue "orders" --processing-record "processing.json" "这个队列有积压吗？"
```

队列查询成功且待投递数或待确认数大于零时，OpsPilot 会尝试读取记录。队列和虚拟主机匹配的记录会保留在报告中；文件缺失、内容无效或目标不匹配时，报告会保留不可用原因。RabbitMQ 队列指标的消息 ID 样本为空，消息关联标为 `unknown`，待投递积压的根因仍标为 `undetermined`。

## RabbitMQ 验收测试

默认 pytest 在离线环境运行。真实验收需要一个独立的 RabbitMQ 测试实例，开启 Management 插件及队列统计。可以在单独的 PowerShell 窗口启动下面的临时容器，启动完成后再运行测试。运行结束后用 Ctrl+C 停止容器。 [RabbitMQ 容器文档](https://www.rabbitmq.com/docs/download)

```powershell
docker run --rm -it -p 127.0.0.1:25672:5672 -p 127.0.0.1:25673:15672 -e RABBITMQ_DEFAULT_USER=opspilot_test -e RABBITMQ_DEFAULT_PASS=opspilot_local_only rabbitmq:4-management
```

在 OpsPilot 仓库根目录运行以下命令。使用已有测试实例时，将两个 URL 和查询凭据替换为该实例的配置。AMQP 用户需要能创建、发布和订阅临时队列。Management 查询凭据可以独立配置。

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev,rabbitmq-test]"
$env:OPSPILOT_TEST_RABBITMQ_AMQP_URL = "amqp://opspilot_test:opspilot_local_only@127.0.0.1:25672/%2F"
$env:OPSPILOT_TEST_RABBITMQ_MANAGEMENT_URL = "http://127.0.0.1:25673"
$env:OPSPILOT_RABBITMQ_USER = "opspilot_test"
$env:OPSPILOT_RABBITMQ_PASSWORD = "opspilot_local_only"
.\.venv\Scripts\python.exe -m pytest tests/test_rabbitmq_live.py --rabbitmq-live -q
```

测试覆盖空队列、有待投递且无订阅、部分及全部消息已投递待确认、业务记录读取，以及按任务 ID 回看报告。每次测试使用 broker 生成的独占队列和临时 SQLite，结束时随连接和临时目录一起清理。
