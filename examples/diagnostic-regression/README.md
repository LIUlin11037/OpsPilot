# 离线回归案例数据

`cases.json` 记录每个案例的输入与预期。`expected` 用报告字段的子集描述要核对的语义，列表的数量和顺序也属于期望。`absent` 列出必须缺失的字段。

## 真实观测的提取

- `healthy` 的 HTTP 200 和 `http-fault` 的 HTTP 504 取自同一隔离 Traefik `orders` 路由的健康基准与故障观测。故障保留 `fault-metrics-raw-error_rate.json` 和 `fault-metrics-raw-latency_p95.json` 中的 7 个原始数值及相对于查询结束的秒数。
- `recovered-history` 的当前 HTTP 200 取自恢复复查。历史指标保留两条 `history-raw-*.json` 的全部采样，窗口长度和采样步长保持为 90 秒和 5 秒。历史 JSONL 取窗口内最先出现的两条目标错误和一条相邻服务正常记录。日期整体迁移到样例日期，保留相对于窗口起点的时间。案例预期只统计这两条保留的错误。
- 保留的是隔离环境的状态、数值、观测相对时间和示例服务名。验收日志里的主机、请求内容和运行产物不随仓库分发。 [原始验收统计与复现条件](../../docs/v1.3-acceptance.md)

## 模拟的边界条件

- `history-source-timeout` 使用 Prometheus 查询超时响应，搭配当前健康和固定窗口内的正常日志。
- `scope-isolation` 增加相邻服务窗口内错误与目标服务窗口外错误。目标服务在窗口内有一条正常记录。
- `unrelated-message` 使用有积压的队列和另一消息 ID 的失败记录。预期查询成功且零匹配，报告不出现该条记录的 ID 或失败状态。
