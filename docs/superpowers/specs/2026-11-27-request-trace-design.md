# OpsPilot v1.6 单次请求的跨服务追踪

## 使用方式

值班人员拿到一个 Trace ID 后，可以查看这次请求经过的服务、各段调用的观测状态和仍缺少的记录。GitHub `main` 的 `9fc7d6b` 是本轮基线。正常入口继续负责预览、执行和保存，接手人按任务 ID 回读报告。

```powershell
python -m opspilot diagnose-auto --config target.json --trace-id TRACE_ID --plan "检查这次请求"
python -m opspilot diagnose-auto --config target.json --trace-id TRACE_ID "检查这次请求"
python -m opspilot report TASK_ID --format markdown
```

## 后端选择

| 方案 | 取舍 |
| --- | --- |
| Jaeger 2，已选 | 正式的 v3 HTTP JSON 接口按 ID 读取。单个 Jaeger 容器可接收并查询隔离服务的跨度。 |
| Grafana Tempo | 正式的 HTTP 接口按 ID 读取，适合已有 Tempo 的环境。将来可以复用同一组规范化证据。 |

本轮按 Jaeger 2.23 的 `GET /api/v3/traces/{traceId}` 验收。[Jaeger 查询接口](https://www.jaegertracing.io/docs/2.23/architecture/apis/) · [接口定义](https://github.com/jaegertracing/jaeger-idl/blob/main/proto/api_v3/query_service.proto) · [Tempo 查询接口](https://grafana.com/docs/tempo/latest/api_docs/)

## 整合方式

- 目标配置增加可选 `jaeger` 段，填写查询地址和可选超时时间。需要令牌时由环境变量提供。地址和预览只记录请求范围，应用入口负责组装连接器。预览列出 Trace ID、单次 GET 和读取上限，运行预览时不访问后端。
- Connector 校验精确的非全零十六进制 ID，并读取有界的 v3 JSON 结果。接受 16 位和 32 位 ID，以统一的 32 位形式匹配跨度。请求默认 5 秒，最长 15 秒，最多读取 4 MiB 和 1,024 段跨度。映射无匹配、权限不足、超时、损坏响应和超过上限。流中已验证的匹配跨度可以随明确的缺口一起返回。
- 将后端结果转为类型明确的跨度记录，包含服务、操作、跨度 ID、父 ID、开始和结束时间、记录状态和读取来源。报告和交接只保存这些取证所需字段。Core 遵循通用跨度契约，原有业务标识追踪仍使用各自的适配器。
- 应用把请求追踪摘要、取证结果和证据索引保存到现有 SQLite 报告。JSON 保留字段，Markdown 列出可读的父子链路、异常及覆盖缺口。已保存的旧报告和 v1.5 回归继续使用现有路径。

## 诊断判断

- 仅使用同一 Trace ID 的跨度和已确认的父子关系。按稳定顺序列出链路，依据父子关系判断上下游。下游和祖先同时报错时，报告标明祖先位于同一异常路径，传播关系作为有条件的解释。独立分支分别引用异常跨度，根因仍待验证。
- 父跨度缺失、重复且冲突的跨度、循环关系或未知字段要保留缺口。混入其他 Trace ID 的跨度不得用于结论。查询成功但无匹配与读取失败分别报告。当前数据不足时保留未知，已读取且能归属的观测仍可回读。
- 正常案例的结果为本次读取范围内未观测到异常。报告的范围以实际采样和存留的记录为准。

## 验收与提交

默认 pytest 用本机固定响应覆盖正常、下游和上游同路径异常、并行错误、缺失父跨度、错 ID、无记录、超时、拒绝访问及上限。另用两个本机隔离的示例 HTTP 服务和单容器 Jaeger 采集正常请求、下游 5xx 和缺失中间跨度三份实际追踪，逐份保存 Trace ID、任务 ID、原始跨度的核对结果和可复现步骤。OpsPilot 侧始终只读。HireHub 源码保持原样，Jaeger 仅用于可选验收。

沿用可回退的小提交，每次提交前运行 `python -m opspilot` 和 `python -m pytest`，最终复跑 v1.5 的离线回归。推送、合并、打标签和发布另行授权。
