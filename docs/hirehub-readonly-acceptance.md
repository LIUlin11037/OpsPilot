# HireHub 投递通知只读验收

源码核对基线为 HireHub `15ee3f9`。2026 年 11 月 19 日已在隔离 HireHub 实例上完成只读验收。仓库中的合成快照可用于离线复现。

## 验收用例

用户提交一个已知 `deliveryId`，描述「投递成功，但 HR 未收到站内通知」。选用预先准备的隔离样本，其中该投递的 `DELIVERY_CREATED` 事件包含预期接收人，Outbox 已标为 `CONFIRMED`，Job 分支有处理记录，而 Notification 分支在约定的观察窗口内尚无处理记录。用观察到的通知队列积压和零注册消费者补充上下文。

验收输入包括测试环境标识和 RabbitMQ 虚拟主机、`deliveryId`、测试 HR 的用户 ID，以及样本的观察窗口。验收读取已有样本，使用只读数据库账号和 RabbitMQ Management GET。记录每次查询的来源和 UTC 观测时间，数据库业务时间按样本环境的已确认时区解释。

## 证据来源

| 检查点 | 读取方式与关联键 | 证据能支持的判断 |
| --- | --- | --- |
| 投递已落库 | `hirehub_delivery.delivery` 按 `id = deliveryId` 查询 `id`、`job_id`、`company_id` 和 `status`。 | 该投递行已存在。 |
| 找到创建事件和接收人 | `hirehub_delivery.local_message` 按 `biz_id = deliveryId`、`biz_type = DELIVERY_CREATED`、`routing_key = delivery.created` 查询。记录 `message_id`、`status`、`send_time`、`ack_time`，从 `payload.receiverIds` 投影预期 HR 是否在列表中。 | 将投递关联到唯一创建事件。接收人判断以创建时保存的负载为准。 |
| Job 分支 | `hirehub_job.mq_consume_log` 按 `message_id` 和 `consumer = job-delivery-count` 查询，保留 `create_time`。 | 该消息有 Job 处理事务记录。若要核实职位计数变化，需要该职位在测试中的前后快照。 |
| Notification 分支 | `hirehub_notification.mq_consume_log` 按同一个 `message_id` 和 `consumer = notification` 查询，保留 `create_time` 或成功查询的零行结果。 | 判断采样时是否观测到通知处理事务。超时或无权限时，分支状态为未知。 |
| 通知队列 | RabbitMQ Management `GET /api/queues/{vhost}/{queue}`，队列为 `hirehub.notification.msg`。复用 OpsPilot 的待投递数、待确认数和消费者注册数。 | 队列整体是否积压及订阅情况。消息是否位于队列中仍需逐消息证据。 |
| 站内通知可见性（辅助） | 通过网关以测试 HR 身份读取 `GET /api/notification/list`，或按已知 `notificationId` 只读核对 `notification` 与 `notification_receiver` 的 `notification_id`、`receiver_id` 和 `deleted`。 | 确认指定通知对测试 HR 的站内可见性。当前通知表缺少可与 `deliveryId` 或 `messageId` 直接关联的字段。 |

使用表中字段和绑定参数查询。Outbox 查到多条创建记录时先标记关联歧义。只记录预期接收人是否命中，样本报告保留业务关联所需的最少字段。选择近期样本，因为 HireHub 定时清理超过 7 天的已确认 Outbox 记录。

## 通过条件

1. 报告从 `deliveryId` 定位唯一 `messageId`，确认负载包含预期 HR，并分别记录 Job、Notification 两个分支的证据。
2. 报告保留 Outbox 的 `CONFIRMED` 和 Job 的已观测记录，同时把 Notification 写为「在观察窗口内未观测到处理记录」。这个结论以通知库查询成功且返回零行为条件。
3. 报告列出通知队列的整体积压与注册消费者数，消息级队列关联及积压根因维持未知。若通知库读取失败，结论说明证据不可用，并保留成功读取的其他证据。
4. 报告带有任务 ID，保存后可按 ID 读回。验收使用只读查询，测试投递状态在验收前后保持一致。

## 源码边界

- `DeliveryService` 和 `LocalMessageRecorder` 在投递事务写入 `DELIVERY_CREATED`。`MqConst`、`JobMqConfig`、`NotificationMqConfig` 将 `delivery.created` 分发到 `hirehub.job.delivery` 和 `hirehub.notification.msg`。
- `DeliveryCountConsumer` 和 `DeliveryNotifyConsumer` 都发布消费回执。`ConsumeAckListener` 只按 `messageId` 和 Outbox 前置状态选择更新行，因此 `CONFIRMED` 无法证明两条分支都已完成。
- `DeliveryNotifyService` 在通知处理事务中写入幂等日志。接收人为空时会跳过通知创建，仍可写入消费日志。`NotificationService` 在提交后推送 WebSocket，因此站内可见性和实时收包需要各自的证据。
- 投递详情 `GET /api/delivery/{id}` 会在 HR 查看时触发 `PENDING` 到 `VIEWED` 的迁移。验收通过 `delivery` 表的只读查询核对状态。

## 使用脱敏快照验收

在 OpsPilot 根目录运行以下命令。样例的投递和消息 ID 为合成标识。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-hirehub --snapshot "examples/hirehub-notification-snapshot.json" --delivery-id 1001 "投递成功，但 HR 未收到站内通知"
```

快照按一个环境、虚拟主机和投递 ID 保存。每个检查点记录 `outcome`，可用 `observed_at` 覆盖共同的带时区 `captured_at`。Job 和 Notification 检查点同时标明查询的 `message_id` 和 `consumer`，行记录也保留这两个键。

报告的 `target_trace.branches` 应为 Job `observed`、Notification `not_observed`。Notification 证据中的 `outcome: "observed"` 表示查询成功，`row_count: 0` 表示没有查到匹配记录。将快照副本中 Notification 的 `outcome` 改为 `timeout` 或 `missing`，再次运行时该分支应为 `unknown`，并保留已经读到的 Job 与 Outbox 证据。缺少查询范围或键不匹配时，该分支也为 `unknown`。

可用 `opspilot report TASK_ID` 回看完整报告。队列指标来自同一份快照，根因仍为 `undetermined`。

## 从隔离环境采集快照

`capture-hirehub` 适用于三个 HireHub schema 位于同一 MySQL 实例、业务时间以 UTC 写入的隔离环境。为 MySQL 账号授予上述四张证据表的 `SELECT` 权限，并为 RabbitMQ Management 账号授予读取通知队列的权限。通过环境变量提供 `OPSPILOT_MYSQL_USER`、`OPSPILOT_MYSQL_PASSWORD`、`OPSPILOT_RABBITMQ_USER` 和 `OPSPILOT_RABBITMQ_PASSWORD`。

在 OpsPilot 根目录运行。将端口、虚拟主机、投递 ID 和预期 HR ID 换成隔离环境的实际值。

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[hirehub]"
New-Item -ItemType Directory -Force .opspilot | Out-Null
.\.venv\Scripts\python.exe -m opspilot capture-hirehub --environment hirehub-local --delivery-id 1001 --recipient-id 4002 --mysql-host 127.0.0.1 --mysql-port 13306 --management-url http://127.0.0.1:15673 --vhost "/" | Out-File -Encoding utf8 .opspilot/hirehub-snapshot.json
.\.venv\Scripts\python.exe -m opspilot diagnose-hirehub --snapshot .opspilot/hirehub-snapshot.json --delivery-id 1001 "投递成功，但 HR 未收到站内通知"
```

采集命令输出最小字段。接收人核对只保留布尔值或未知值。各来源分别记录读取时间和结果，单项查询失败时保留其他可读取的证据。

新采集的每个检查点会记录 `duration_ms`，使用单调时钟测量 MySQL 查询或 RabbitMQ Management 读取的耗时。超时和读取失败同样保留耗时。读取快照时，报告复用采集时的耗时。早期快照没有该字段时，证据省略它。`tool_calls` 中的耗时仍表示本次整体取证调用。

采集失败时，快照和诊断报告会记录 `reason`，例如 `access_denied`、`authentication_failed`、`transport_error` 或 `request_timeout`。有可用的错误码时，也会记录 `mysql_error_code` 或 `http_status`。快照仅保留受支持的原因和数字错误码。早期快照缺少这些字段时，报告沿用 `source_unavailable` 或 `source_timeout`。

## 直接诊断隔离环境

使用相同的依赖和只读凭据，`diagnose-hirehub` 可在一次运行中采集并保存报告。

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose-hirehub --environment hirehub-local --delivery-id 1001 --recipient-id 4002 --mysql-host 127.0.0.1 --mysql-port 13306 --management-url http://127.0.0.1:15673 --vhost "/" "投递成功，但 HR 未收到站内通知"
```

实时报告直接记录 MySQL 表和 RabbitMQ Management 请求地址。实时诊断和离线回放都会保留已采集的队列检查点，即使当前无法关联唯一的 Outbox 消息。队列指标反映整个队列，消费分支状态在消息关联建立前为 `unknown`。需要保存原始快照以便复查时，使用 `capture-hirehub` 导出。

## 对比复查前后的报告

对同一消息提交两次诊断后，用 `reports --delivery-id` 查找任务 ID。也可以用 `--environment` 按环境精确筛选。带筛选的列表会返回 `target_trace`，包含环境、虚拟主机、投递 ID、消息 ID 和消费分支状态。`--limit` 应用于筛选后的结果。

```powershell
.\.venv\Scripts\python.exe -m opspilot reports --delivery-id 1001 --environment hirehub-local --limit 5
```

把选中的任务 ID 按基准、复查的顺序传给 `compare-hirehub`。两份报告须保存在同一个 SQLite 数据库，并匹配环境、虚拟主机、投递 ID 和已定位的消息 ID。

```powershell
.\.venv\Scripts\python.exe -m opspilot compare-hirehub "BEFORE_TASK_ID" "AFTER_TASK_ID"
```

输出的 `changes` 只列出有变化的分支状态、队列读取结果和队列指标。`null` 表示指标不可用，`0` 表示实际观测到零。队列指标表示队列整体状态，分支状态按消息 ID 关联。

## 重跑直连验收

使用一个已准备好的隔离投递用例和前述只读凭据。在运行测试期间保持 Notification 的状态稳定。验收测试读取单个投递与通知队列，报告写入 pytest 临时目录。

```powershell
$env:OPSPILOT_TEST_HIREHUB_MANAGEMENT_URL = "http://127.0.0.1:15673"
$env:OPSPILOT_TEST_HIREHUB_MYSQL_PORT = "13306"
$env:OPSPILOT_TEST_HIREHUB_DELIVERY_ID = "1001"
$env:OPSPILOT_TEST_HIREHUB_RECIPIENT_ID = "4002"
$env:OPSPILOT_TEST_HIREHUB_MESSAGE_ID = "<该投递的 Outbox 消息 ID>"
$env:OPSPILOT_TEST_HIREHUB_NOTIFICATION = "not_observed"
.\.venv\Scripts\python.exe -m pytest tests/test_hirehub_live.py --hirehub-live -q
```

默认连接本机 MySQL 和 RabbitMQ 的 `/` 虚拟主机。可使用 `OPSPILOT_TEST_HIREHUB_MYSQL_HOST` 与 `OPSPILOT_TEST_HIREHUB_VHOST` 指定其他隔离环境。Notification 暂停且消息仍积压时，用 `not_observed` 验证授权查询返回零行；换成缺少通知日志表读取权限的账号时，用 `unknown` 验证查询失败；恢复消费且队列清空后，用 `observed` 验证处理记录。三种场景分别运行测试，默认 pytest 跳过此验收。

## 真实验收结果

隔离环境运行 `15ee3f9` 的 Delivery、Job、Notification，以及 MySQL 8.0.46、Redis 和 RabbitMQ 3.13.7。预置合成投递及 Outbox 记录后，由真实服务发送和消费消息。Elasticsearch 索引的启动检查使用本地测试响应。

首轮验收通过临时脚本导出快照。随后以 OpsPilot `416e185` 的 `capture-hirehub` 在相同版本的隔离环境复验，通过 MySQL `SELECT` 和 RabbitMQ Management `GET` 直接采集三份快照，并用 `diagnose-hirehub` 生成报告。

OpsPilot `27ecb1a` 的实时 `diagnose-hirehub` 入口已通过 `--hirehub-live` 的三轮复验。三轮都直接连接隔离服务，从实时证据生成和保存报告。

| 场景 | 观测结果 | Notification 分支 |
| --- | --- | --- |
| 暂停 Notification | Outbox 为 `CONFIRMED`，Job 投递计数增加 1，通知查询零行，队列待投递 1 条且消费者为 0 | `not_observed` |
| 通知日志表拒绝查询 | MySQL 返回 `1142`，仍可读取 Job 和 Outbox | `unknown` |
| 恢复 Notification | 查到 1 条消费记录和 1 条收件人记录，待投递数归零 | `observed` |

三份诊断报告均可按任务 ID 回读，投递状态在诊断前后均为 `PENDING`。
