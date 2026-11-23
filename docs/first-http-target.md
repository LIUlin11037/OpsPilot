# 接入第一个 HTTP 诊断目标

准备一个可以从 OpsPilot 主机访问的 HTTP(S) 健康端点。确认该端点支持只读 GET，并了解它只检查进程存活，还是也检查数据库等依赖。需要认证的环境使用你有权访问、能提供只读观测的入口。当前 HTTP 检查只读取状态码，配置 URL 不支持凭据、查询参数或片段。

先按 README 安装并确认应用能启动。下列命令从仓库根目录执行，使用 Windows PowerShell。其他系统可以将 `$python` 指向该项目虚拟环境的 Python。[安装与启动](../README.md)

## 配置和预览

在仓库根目录新建一个首次接入目录，将样例复制进去。如果下方目录已用于其他验收，给 `$work` 换一个新路径。

```powershell
$python = ".\.venv\Scripts\python.exe"
$work = ".opspilot/first-http"
New-Item -ItemType Directory -Force $work | Out-Null
$target = Join-Path $work "target.json"
$db = Join-Path $work "reports.sqlite3"
Copy-Item examples/http-target.json $target
```

用编辑器打开 `$target` 指向的文件，替换 `health.service_url` 为实际健康端点，保存为 UTF-8。样例只需要这一项。如果另有独立的 HTTP 依赖健康端点，可以在同一 `health` 对象中加一个 `dependency_urls` 数组。

```powershell
& $python -m opspilot --db $db diagnose-auto --config $target --workflow http_health --plan "检查服务健康"
```

先核对 `scope.service_url` 和 `checks` 中的来源是否都在授权范围内。预览只读取本地配置，可以在目标暂时不可访问时运行。任何配置错误都应先修正，再开始采集。

## 执行并回读

```powershell
$baseline = & $python -m opspilot --db $db diagnose-auto --config $target --workflow http_health "记录健康基准" | ConvertFrom-Json
& $python -m opspilot --db $db report $baseline.task_id --format markdown
```

报告保存在 `$db` 指向的 SQLite 文件。Markdown 显示主服务地址、观察时间、工具结果和建议的下一步。保留 `$baseline.task_id`。如果关闭了 PowerShell 窗口，可以用 `reports --limit 5` 在同一个数据库中重新查找任务 ID。

## 故障与恢复对比

在隔离环境复现故障，或在实际问题发生时做一次只读采集。目标恢复后使用相同配置和报告库复查。

```powershell
# 故障期间
$fault = & $python -m opspilot --db $db diagnose-auto --config $target --workflow http_health "复查故障期间的服务健康" | ConvertFrom-Json
& $python -m opspilot --db $db report $fault.task_id --format markdown

# 目标恢复后
$recovery = & $python -m opspilot --db $db diagnose-auto --config $target --workflow http_health "复查恢复后的服务健康" | ConvertFrom-Json
& $python -m opspilot --db $db compare-http $baseline.task_id $fault.task_id --format markdown
& $python -m opspilot --db $db compare-http $fault.task_id $recovery.task_id --format markdown
```

若显式配置了独立依赖，健康主服务的默认复查可能跳过它们。需要确认依赖也恢复时，执行恢复诊断时加 `--check-dependencies`，再核对每个依赖的本次观测。

## 看懂失败提示

| 看到的提示或证据 | 怎么处理 |
| --- | --- |
| `unsupported fields` 后列出了 `servce_url` 等字段 | 核对样例和拼写。`dependency_urls[1]` 指向数组中第二个 URL。配置错误在读取端点前返回。 |
| 主服务 `transport_error` 或 `request_timeout` | 从 OpsPilot 所在主机核对报错来源的主机、端口、网络或 TLS。报告已保存，健康状态为 `unknown`。 |
| 某个依赖不可读取 | 按任务 ID 回看报告，核对依赖来源和失败原因。主服务及其他已完成的检查仍可回看。 |
| 端点返回 `424` 或 `5xx` | 报告记录为已观测到端点异常。检查端点和服务日志，具体故障组件需要独立证据。 |
| 端点返回 `401` 或 `403` | 检查是否具备该只读观测入口的访问权限。报告保留响应码及权限原因。 |

## 当前组件覆盖

| 入口 | 实际观测范围 |
| --- | --- |
| HTTP(S) 健康端点 | 实时 GET 状态码。配置独立的依赖健康端点后，可逐个观察。 |
| Redis、MySQL 或 MariaDB | Redis 实时 PING。MySQL 协议连接使用只读账号执行 `SELECT 1`。 |
| RabbitMQ Management API | 实时队列待投递、待确认和消费者观测。另配处理记录来源后，可以按消息 ID 查询。 |
| 本地 JSONL 服务日志 | 读取配置的日志文件，统计观察窗口内的错误记录。 |
| HireHub、Voting 示例 | HireHub 投递通知支持已验收的实时只读查询及快照回放。Voting 以采集的快照追踪投票处理。 |

可从 HTTP 健康端点间接观察其他组件，例如由应用的健康端点检查 PostgreSQL。当前通用 HTTP 报告的证据粒度是端点状态码。[现有 CLI 与连接器用法](../README.md)

要在隔离环境演练一次完整接入，可以使用仓库中的 Gitea 和 PostgreSQL 验收环境。[v0.2 验收步骤](v0.2-onboarding-acceptance.md)
