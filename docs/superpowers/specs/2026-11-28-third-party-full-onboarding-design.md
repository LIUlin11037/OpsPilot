# OpsPilot v1.9 第三方项目的全链路独立接入验收

验证首次使用者能从一套第三方服务的现有观测数据出发，完成故障窗口调查、请求追踪与离线报告对比。使用本地已验证的 v1.8 `a3c4217` 作为基线。

## 接入边界

| 方案 | 取舍 |
| --- | --- |
| 接入侧映射日志字段，已选 | 可以复用项目原有访问日志，保留原始记录以便逐条核对。 |
| 只填目标配置 | 步骤最少，但项目需要原生生成与 OpsPilot 字段和时间格式一致的 JSONL。 |

建议以 **Caddy v2.11.9** 的网关和上游服务组成轻量双服务拓扑。计划用官方镜像运行原版程序，记录复现用的镜像摘要。Caddy 可提供带 `traceID` 的结构化访问日志、原生 Prometheus 指标和 OpenTelemetry 追踪。网关与上游会通过可验证的请求上下文关联到 Jaeger。此次验收范围是 HTTP 网关及上游请求。 [Caddy 版本](https://github.com/caddyserver/caddy/releases/tag/v2.11.9) · [访问日志和追踪](https://caddyserver.com/docs/caddyfile/directives/tracing) · [指标](https://caddyserver.com/docs/metrics)

## 配置与取证

- 隔离环境使用两实例 Caddy、Prometheus 和 Jaeger。测试流量经过网关到上游。故障阶段在上游配置中返回可验证的 5xx，恢复后返回 200。采集组件与造故障操作归验收环境，OpsPilot 仅访问已配置的查询和日志输入。
- 以网关原生访问日志为来源。将 `ts` 和 `traceID` 映射到 JSONL 的 `timestamp` 与 `trace_id`，将服务名对应到 Prometheus 的 `job`。根据原始 HTTP 状态码标记错误级别，保留原始码。日志不含可用 Trace ID 时保留缺口，保留仅验收用的原始日志副本。 [Caddy 结构化日志](https://caddyserver.com/docs/logging)
- Prometheus 抓取网关原生指标。选取一个明确的处理层，依据原指标的状态码标签映射 `status`，以避免多个中间件重复计数。查询结果与原始响应对照。Jaeger 2.23.0 接收真实追踪，OpsPilot 用现有 HTTP 接口按准确 Trace ID 查询。 [Prometheus 指标标签配置](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#metric_relabel_configs) · [Jaeger 接收与查询环境](https://www.jaegertracing.io/docs/2.23/getting-started/)
- 先核对故障窗口的 HTTP、日志、指标和跨度。端点恢复后复查原窗口，再调查后续健康窗口。用一例日志或追踪不可用核对缺口并保留其余观测。保存原始观测、配置、任务 ID、两组恢复对比和缺口对比。停止隔离观测服务后再执行 `compare-incident`。

## 独立验收

先自测并冻结首次接入指南，再由未参与编写的执行者仅凭指南、接入配置和就绪的只读来源完成诊断。计时从收到可访问环境开始，到按任务 ID 读出首份报告为止，目标 30 分钟以内。记录耗时、改动和障碍。接入验收与兼容性修复按独立小提交交付，提交前运行启动和全量 pytest，复验 v1.5 与 v1.8。推送另行授权。
