# HireHub 投递通知只读验收

源码核对基线为 HireHub `15ee3f9`。本用例待在隔离实例执行，以下为验收设计与源码证据。

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

OpsPilot 已具备 RabbitMQ 只读观测、按消息 ID 查处理记录和报告回看。接入此用例还需要 HireHub 侧的只读证据导出或独立适配器，用于解析 `deliveryId`、提供两个分支的查询结果，并区分成功查询的零行与数据源不可用。
