# OpsPilot

OpsPilot 面向异构微服务的只读故障诊断。CLI 支持消息积压、HireHub 投递通知追踪和 HTTP 健康端点诊断。

在仓库根目录使用 Python 3.10 或更高版本运行以下命令（Windows PowerShell）。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m opspilot
.\.venv\Scripts\python.exe -m pytest
```

启动成功时输出 `OpsPilot is ready.`。安装依赖后，启动和测试均可离线运行。

## 按故障描述选择诊断流程

`diagnose-auto` 根据描述中的中英文症状词，在 HTTP 健康检查和 RabbitMQ 队列诊断之间选择一个已配置的流程。HTTP 流程使用配置的服务和依赖端点，队列流程使用指定的 Management API 和虚拟主机。命令只调用选中的流程，并在报告的 `routing` 记录流程名称和匹配词。

把常用目标保存为 UTF-8 的 `target.json`。可以按需配置 `health`、`rabbitmq` 和 `hirehub`。`health` 使用 HTTP 健康配置字段，`rabbitmq` 需要管理地址与队列名，虚拟主机默认为 `/`，超时默认为 5 秒。RabbitMQ 和 MySQL 凭据通过环境变量提供。

```json
{
  "health": {
    "service_url": "http://127.0.0.1:8080/health",
    "dependency_urls": ["http://127.0.0.1:8081/health"]
  },
  "rabbitmq": {
    "management_url": "http://127.0.0.1:15672",
    "queue": "orders",
    "vhost": "/"
  }
}
```

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-auto --config .\target.json "RabbitMQ 队列积压"
```

可在 `rabbitmq` 段添加 `consumer_health_url`，并通过 `processing_url` 或 `processing_record` 配置一个处理记录来源。观察到待投递或待确认消息时，队列流程会检查已配置的消费者端点，并按队列和虚拟主机读取最新处理记录。`processing_record` 的相对路径以目标配置文件所在目录为基准。

配置了处理记录来源后，可以通过 `--message-id` 指定一条消息。OpsPilot 会选择队列流程，按队列、虚拟主机和消息 ID 精确查询。即使队列为空或指标不可用，也会尝试读取该消息的处理记录。报告会记录所请求的 ID 及选择依据。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-auto --config .\target.json --message-id "msg-101" "这条消息处理了吗？"
```

追踪 HireHub 投递时，可以在目标配置的 `hirehub.snapshot` 指定本地快照。`--delivery-id` 选择快照追踪流程。相对路径以配置文件所在目录为基准。若把以下配置保存到仓库根目录，就可以使用随仓库提供的样本。

```json
{
  "hirehub": {
    "snapshot": "examples/hirehub-notification-snapshot.json"
  }
}
```

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-auto --config .\target.json --delivery-id 1001 "投递成功后没收到通知"
```

临时排查也可以直接提供连接选项。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-auto --health-config .\health.json --management-url http://127.0.0.1:15672 --queue "orders" "RabbitMQ 队列积压"
```

描述同时命中两类症状、没有命中或缺少所选流程的配置时，命令会提示补充。可以聚焦一种症状后重试，也可以用 `diagnose-http` 或 `diagnose` 明确指定流程。自动选择只识别固定症状词，选中的流程仍按观测证据决定后续检查。

使用 `--plan` 可预览选中的流程、查询范围和检查条件。预览根据本地配置生成 JSON，连接可用性和数据来源在执行诊断时验证。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-auto --config .\target.json --plan "RabbitMQ 队列积压"
```

## HTTP 健康诊断

将服务和可选依赖的健康检查 URL 提供给 `diagnose-http`。默认在服务端点异常或健康状态未知时检查已配置的依赖。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-http --service-url http://127.0.0.1:8080/health --dependency-url http://127.0.0.1:8081/health "服务健康检查失败，依赖能访问吗？"
```

请配置以 HTTP 状态码表达健康的端点。命令通过 GET 读取端点的原始响应，`2xx` 记为 `healthy`，`5xx` 记为 `unhealthy`，其余状态、超时和连接失败记为 `unknown`。报告记录端点、观察时间、状态码和调用耗时。服务异常时，依赖结果可缩小核查范围，诊断状态为 `undetermined`。报告保存后可按任务 ID 回看。

运行 `python -m pytest tests/test_http_health.py -q` 可用自动启停的本地 HTTP 服务复跑这条诊断流程。

主服务或依赖端点返回 `5xx` 时，报告会建议查看该端点在观测时间附近的请求日志和错误日志。Markdown 报告会在建议旁标明端点 URL，便于区分多个依赖。

依赖是 Redis 时，可指定一个允许直接执行 `PING` 的 `redis://HOST[:PORT]` 端点。收到 `PONG` 时记录 Redis 端点可响应。超时、认证拒绝和异常响应记为 `unknown`。 [Redis PING 文档](https://redis.io/docs/latest/commands/ping/)

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-http --service-url http://127.0.0.1:8080/health --redis-dependency-url redis://127.0.0.1:6379 "服务健康检查失败，Redis 能响应吗？"
```

依赖是 MySQL 时，安装 `.[hirehub]` 可选依赖，并通过 `OPSPILOT_MYSQL_USER` 和 `OPSPILOT_MYSQL_PASSWORD` 提供只读账号。`--mysql-dependency-url` 接收 `mysql://HOST[:PORT]`，对端点执行 `SELECT 1` 核实连接和查询响应。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-http --service-url http://127.0.0.1:8080/health --mysql-dependency-url mysql://127.0.0.1:3306 "服务异常，MySQL 能响应吗？"
```

三种依赖参数可以同时使用，也可以分别重复传入。依赖按 HTTP、Redis、MySQL 的顺序检查，组内按传入顺序执行。每个端点分别使用 `--timeout`，报告按调用顺序保存各自的来源、结果和耗时。

### 复用本地配置

将端点和超时保存到 UTF-8 JSON 文件，例如 `health.json`。`service_url` 必填，三个依赖数组默认为空，`timeout_seconds` 默认为 5。使用 `--config` 时，服务、依赖和超时均从文件读取。

```json
{
  "service_url": "http://127.0.0.1:8080/health",
  "dependency_urls": ["http://127.0.0.1:8081/health"],
  "redis_dependency_urls": ["redis://127.0.0.1:6379"],
  "mysql_dependency_urls": ["mysql://127.0.0.1:3306"],
  "timeout_seconds": 2
}
```

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-http --config .\health.json --check-dependencies "复查服务和依赖"
```

`--check-dependencies` 适用于配置文件和直接传入 URL 两种方式。启用后，即使主服务健康，也会检查所有已配置的依赖。任一已检查的依赖异常或状态未知时，报告的诊断状态为 `undetermined`。

### 对比复查报告

用 `compare-http-latest --service-url` 比较同一 SQLite 数据库中该主服务最近保存的两份报告。较早的一份是基准，最新的一份是复查。URL 必须与报告中记录的主服务 URL 完全一致。

```powershell
.\.venv\Scripts\python.exe -m opspilot compare-http-latest --service-url http://127.0.0.1:8080/health --format markdown
```

要比较指定报告，可用 `reports --service-url` 查找任务 ID，再按基准、复查的顺序传给 `compare-http`。列表从最新到最早排列，`--limit` 控制返回的匹配数量。

```powershell
.\.venv\Scripts\python.exe -m opspilot reports --service-url http://127.0.0.1:8080/health --limit 2
.\.venv\Scripts\python.exe -m opspilot compare-http "BEFORE_TASK_ID" "AFTER_TASK_ID" --format markdown
```

Markdown 表格并排显示基准和复查观测，包括健康状态、HTTP 状态码、失败原因以及已记录的 MySQL 错误码。默认输出 JSON，`changes` 列出变化的端点观测。任一侧缺少观测时，Markdown 显示 `No observation recorded`，JSON 以 `null` 表示。相同角色和 URL 的重复检查按 `occurrence` 从 1 开始匹配。

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

新报告的 `next_checks` 给出供人工执行的只读核查建议。每项用从 0 开始的 `evidence_index` 指向触发建议的 `evidence` 项。权限不足时建议核对只读账号的访问范围，超时或分支查询零行时建议重新采样。Markdown 报告会列出建议对应的检查点。

## 回看诊断报告

`diagnose` 会先把报告保存到当前目录下的 `.opspilot/reports.sqlite3`，然后输出包含 `task_id` 的 JSON。

可以先用 `reports` 找到任务 ID。列表按保存顺序从新到旧返回任务 ID、问题和诊断状态，默认最多 20 条。没有报告时返回空列表。

查找 HireHub 复查记录时，可以按 `--delivery-id` 或 `--environment` 筛选。 [HireHub 报告对比](docs/hirehub-readonly-acceptance.md#对比复查前后的报告)

```powershell
.\.venv\Scripts\python.exe -m opspilot reports --limit 5
```

用列表中的任务 ID 替换 `TASK_ID`，即可读取完整报告。

```powershell
.\.venv\Scripts\python.exe -m opspilot report "TASK_ID"
```

加上 `--format markdown` 可以查看适合阅读和复制的报告，包含结论、观察时间、证据来源和已有的工具调用记录。队列诊断与 HireHub 诊断共用此入口，默认输出为 JSON。

```powershell
.\.venv\Scripts\python.exe -m opspilot report "TASK_ID" --format markdown
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

### 对比队列复查报告

已有同一队列的多份报告时，可以按报告中队列证据的 `source` 查找历史，或直接对比最近保存的两份。列表从最新到最早排列，对比以较早的一份为基准。

```powershell
$queueSource = "http://127.0.0.1:15672/api/queues/%2F/orders"
.\.venv\Scripts\python.exe -m opspilot reports --queue-source $queueSource --limit 2
.\.venv\Scripts\python.exe -m opspilot compare-queue-latest --queue-source $queueSource --include-consumer-health --format markdown
```

将同一队列的两份诊断报告按基准、复查的顺序传给 `compare-queue`。命令读取同一 SQLite 数据库，要求两份报告的队列来源完全一致，包括 RabbitMQ 管理地址、虚拟主机和队列名。

```powershell
.\.venv\Scripts\python.exe -m opspilot compare-queue "BEFORE_TASK_ID" "AFTER_TASK_ID"
```

`changes` 列出发生变化的队列指标和读取结果。计数为 `0` 表示实际观测到零，`null` 表示当次读不到该指标。待投递数缺失时，报告中仍有有效值的待确认数和消费者注册数也会参与对比。

加上 `--include-consumer-health`，输出会同时包含 `consumer_health_changes`。对比按消费者端点 URL 匹配，列出健康状态、HTTP 状态码或读取结果的变化。若某次报告缺少对应端点观测，该侧显示 `null`，例如队列清空后跳过了健康检查。

默认输出 JSON。加上 `--format markdown` 可按表格查看变化。不可用的队列指标显示为 `Unavailable`，缺少的端点观测显示为 `No observation recorded`。

```powershell
.\.venv\Scripts\python.exe -m opspilot compare-queue "BEFORE_TASK_ID" "AFTER_TASK_ID" --include-consumer-health --format markdown
```

### 查看消费者端点健康

已知消费者的 HTTP 健康端点时，可在 RabbitMQ 模式加上 `--consumer-health-url`。观察到待投递或待确认消息时，OpsPilot 发送只读 GET，记录端点健康状态、观测时间和耗时。待投递数缺失时，已观测到的待确认消息也能触发检查。健康状态说明该端点当时的响应，积压原因仍需结合队列和业务证据核查。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose --management-url http://127.0.0.1:15672 --queue "orders" --consumer-health-url http://127.0.0.1:8081/health "队列有积压，消费者端点能响应吗？"
```

### 结合业务处理记录

目标系统可以用 UTF-8 JSON 导出单条处理记录，也可以将多条相同结构的记录放在一个 JSON 数组中。将下例保存为 `processing.json`，并用实际的队列、消息 ID、结果和时间替换示例值。`result` 支持 `succeeded` 和 `failed`，`attempted_at` 使用带时区的 ISO 8601 时间。

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

观测到待投递数或已投递待确认数大于零时，OpsPilot 会读取同一队列和虚拟主机下的最新记录。待投递数不可用时，已投递待确认数仍可触发查询，报告会保留已观测的计数并将积压诊断状态记为 `undetermined`。加上 `--message-id` 可独立查询指定消息，包括空队列和队列指标不可用的情况。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose --management-url http://127.0.0.1:15672 --queue "orders" --processing-record "processing.json" --message-id "order-msg-101" "查这条消息的处理结果"
```

指定 ID 时，报告会保存 `requested_message_id`。查询在对应队列和虚拟主机内精确匹配 ID，选取 `attempted_at` 最新的一次尝试。同一时间的多条记录取文件位置靠后的记录。读取成功但没有匹配时，证据记为 `observed`，并保留 `row_count: 0`。指定 ID 时原因是 `message_not_found`。文件缺失记为 `missing`，原因为 `record_file_missing`。内容无效或目标不匹配各有对应的不可用原因。

RabbitMQ 队列指标的消息 ID 样本为空，因此报告的队列消息关联仍标为 `unknown`。

### 连接 HTTP 目标服务

用 `--processing-url` 替换 `--processing-record`，即可从目标服务读取同样格式的业务记录。`--timeout` 同时应用于 RabbitMQ 和目标服务的 socket 操作。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose --management-url http://127.0.0.1:15672 --queue "orders" --processing-url http://127.0.0.1:8080/processing --message-id "order-msg-101" "查这条消息的处理结果"
```

OpsPilot 向指定端点发送只读 GET，请求参数为 `queue`、`vhost`，以及用户指定时的 `message_id`。端点使用 HTTP 200 返回上述单条 JSON 记录或记录数组，查询成功但无匹配时返回 HTTP 200 和空数组。OpsPilot 校验返回记录的队列、虚拟主机和消息 ID。HTTP 404、超时和无效响应会作为不可用证据写入报告。当前适配器直接访问无需凭据的测试端点。

### Java 目标服务示例

仓库中的独立 Java 示例用内存样例记录验证 HTTP 契约，需要 PATH 中的 JDK 17 或更高版本。在单独的终端从仓库根目录启动，传入端口、队列和虚拟主机，服务会打印回环地址。上面的 `--processing-url` 示例可以直接查询它。用 Ctrl+C 停止服务。 [JDK 单文件启动说明](https://docs.oracle.com/en/java/javase/17/docs/specs/man/java.html#using-source-file-mode-to-launch-single-file-source-code-programs)

```powershell
java --add-modules jdk.httpserver examples/java-processing-target/ProcessingTarget.java 8080 "orders" "/"
```

只验证 Java HTTP 接口时，可运行下列命令。测试会自动启动和关闭 Java 服务。

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_java_processing_target.py --java-target -q
```

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

测试覆盖空队列、有待投递且无订阅、部分及全部消息已投递待确认和队列清空。验收还会对比清空前后的队列报告，并验证消费者健康端点从 `503` 恢复到 `204`、清空后跳过健康检查、读取业务记录及按任务 ID 回看报告。每次测试使用 broker 生成的独占队列和临时 SQLite，结束时随连接和临时目录一起清理。

在同一套验收中使用 Java 目标服务时，加上 `--java-target`。

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_rabbitmq_live.py --rabbitmq-live --java-target -q
```

HireHub 的投递通知用例已通过隔离环境验收。`diagnose-hirehub` 支持直接诊断隔离环境或读取本地快照，`capture-hirehub` 可单独导出证据。 [HireHub 只读验收](docs/hirehub-readonly-acceptance.md)
