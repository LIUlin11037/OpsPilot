# 首次接入 Caddy 的故障调查

开始前确认 OpsPilot 已安装，拿到网关、Prometheus 和 Jaeger 的只读地址，以及已导出的 Caddy 网关访问日志和历史窗口。隔离示例的地址和窗口会放在 `handoff.json`，访问日志为 `gateway-raw.jsonl`。该示例的回环地址无需令牌。带鉴权的环境使用已有只读令牌，分别通过 `OPSPILOT_PROMETHEUS_TOKEN` 和 `OPSPILOT_JAEGER_TOKEN` 提供。

## 准备目标配置

以下命令在仓库根目录执行。把 `$inputs` 换成收到的输入目录，`$work` 选一个新的输出目录。

```powershell
$inputs = Resolve-Path .opspilot/caddy-inputs
$work = New-Item -ItemType Directory -Path .opspilot/first-caddy
$handoff = Get-Content (Join-Path $inputs 'handoff.json') -Raw | ConvertFrom-Json
$config = Join-Path $work.FullName 'target.json'
$log = Join-Path $work.FullName 'gateway.jsonl'
$db = Join-Path $work.FullName 'reports.sqlite3'
Copy-Item examples/caddy-onboarding/target.json $config
python examples/caddy-onboarding/map_access_logs.py (Join-Path $inputs 'gateway-raw.jsonl') $log --service $handoff.service
```

查看 `target.json` 并核对 `health.service_url`、`prometheus.base_url`、`prometheus.service`、`jaeger.base_url`，依次对应交接的 `gateway`、`prometheus`、`service`、`jaeger`。如果地址与示例不同，按交接值修改。`health.service_log_path` 相对于配置文件所在目录。Prometheus 使用交接中已配置的 `job` 和状态码标签，示例已在抓取时将 Caddy 的 `code` 映射为 `status`。

日志脚本将 Caddy 原有时间、状态码和 `traceID` 写成 OpsPilot JSONL。原始 HTTP 5xx 会映射为错误；没有 Trace ID 的行仍保留 HTTP 观测。示例使用一个已导出的窗口快照，限制在 OpsPilot 支持的 1 MiB 取得范围内。

## 预览、执行和回读

```powershell
$scope = @('--config', $config, '--incident-start', $handoff.incident_start, '--incident-end', $handoff.incident_end, '--follow-traces')
python -m opspilot --db $db diagnose-auto @scope --plan 'Investigate Caddy gateway'
python -m opspilot --db $db diagnose-auto @scope 'Investigate Caddy gateway'
python -m opspilot --db $db report TASK_ID --format markdown
```

把 `TASK_ID` 换成第二条命令返回的 ID。预览显示服务、窗口及最多 3 次按 Trace ID 的只读查询。报告分开显示当前 HTTP 和历史异常。追踪条目中的日志行属于转换后的 `gateway.jsonl`，本次查询及跨度索引可对照同一份报告的证据表。若日志、指标或追踪查不到，先查看报告里的来源和缺口原因。

已有两个同一目标的历史调查任务时，可以离线执行下列对比。每个证据索引引用自己的任务 ID。

```powershell
python -m opspilot --db $db compare-incident BEFORE_ID AFTER_ID --format markdown
```
