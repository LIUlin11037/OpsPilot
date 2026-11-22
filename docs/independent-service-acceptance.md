# 独立服务接入验收

2026 年 11 月 22 日，使用 OpsPilot `149162b` 在 Windows Docker Desktop 上验收。目标为 Docker Awesome Compose 的 NGINX、Go、MariaDB 博客示例，检出版本为 `503c0dcef31b07fb77b2d4c5556bfafdf86f4f1a`。示例的 HTTP 首页通过查询博客标题返回业务响应。OpsPilot 使用 HTTP GET 和 `SELECT 1` 读取服务与数据库状态。 [上游示例](https://github.com/docker/awesome-compose/tree/503c0dcef31b07fb77b2d4c5556bfafdf86f4f1a/nginx-golang-mysql)

## 验收结果

| 阶段 | NGINX / Go HTTP | MariaDB 观测 | 诊断状态 |
| --- | --- | --- | --- |
| 健康基准 | 200 | 查询成功，`healthy` | `no_failure_observed` |
| 停止示例数据库 | 500 | 连接失败，`unknown`，错误码 `2003` | `undetermined` |
| 重启示例数据库 | 200 | 查询成功，`healthy` | `no_failure_observed` |

三次诊断均从故障描述选中 `http_health`。故障阶段从 HTTP 500 触发数据库检查，健康基准和恢复阶段显式复查依赖。三个结果保存在同一 SQLite 数据库，`compare-http-latest` 输出了故障与恢复之间的 HTTP 状态、数据库证据和整体诊断状态变化。依赖恢复后，复查沿用同一个数据库端点。

OpsPilot 侧只提供目标配置和探针账号。上游源码、OpsPilot Core 和适配器均无需修改。这次验证覆盖通用 HTTP 和数据库诊断。针对某个业务对象追踪跨服务处理记录，需要另行定义证据来源和关联规则。

## 复现方法

1. 在独立 Compose 项目中启动上游示例。用外部 Compose 覆盖文件将 NGINX 和 MariaDB 发布到 `127.0.0.1` 上的可用固定端口。覆盖 NGINX 原本的端口列表，例如下方的 `!override`。数据库也使用固定端口，因为容器停止再启动时自动分配的端口可能变化。 [Compose 覆盖语法](https://docs.docker.com/reference/compose-file/merge/#replace-value)

   ```yaml
   services:
     proxy:
       ports: !override
         - "127.0.0.1:18080:80"
     db:
       ports:
         - "127.0.0.1:13306:3306"
   ```

2. 在示例数据库中创建一个只用于健康查询的账号。`SELECT 1` 无需业务表权限。安装 OpsPilot 的 `.[hirehub]` 可选依赖以使用 MySQL 驱动，并通过 `OPSPILOT_MYSQL_USER` 和 `OPSPILOT_MYSQL_PASSWORD` 提供探针账号。在 OpsPilot 仓库根目录保存以下 `target.json`，端口与 Compose 覆盖一致。

   ```json
   {
     "health": {
       "service_url": "http://127.0.0.1:18080/",
       "mysql_dependency_urls": ["mysql://127.0.0.1:13306"],
       "timeout_seconds": 8
     }
   }
   ```

3. 先记录健康基准，再停止该示例的 `db` 容器并记录故障。启动 `db` 并等待其健康检查通过后，记录恢复并生成对比。每条诊断命令使用同一数据库文件。

   ```powershell
   python -m opspilot --db .opspilot/cross-target.sqlite3 diagnose-auto --config target.json --check-dependencies "HTTP health baseline"
   # 停止隔离示例的 db 服务
   python -m opspilot --db .opspilot/cross-target.sqlite3 diagnose-auto --config target.json "HTTP health failure"
   # 启动同一个 db 服务并等待其健康检查通过
   python -m opspilot --db .opspilot/cross-target.sqlite3 diagnose-auto --config target.json --check-dependencies "HTTP health recovery"
   python -m opspilot --db .opspilot/cross-target.sqlite3 compare-http-latest --service-url http://127.0.0.1:18080/ --format markdown
   ```

首次试验使用 3 秒超时，HTTP 探针先超时，随后示例后端才记录 500。将超时调至 8 秒后，正式验收从 HTTP 响应中观测到 500。

## 投票业务链路验收

同日在独立 Compose 项目中运行 Docker Example Voting App 的官方 Vote 和 Worker 镜像，以及 Redis 和 PostgreSQL。投票入口通过 Redis `votes` 列表交给 Worker，Worker 按投票人 ID 在 PostgreSQL 的 `public.votes` 表写入记录。 [上游投票入口](https://github.com/dockersamples/example-voting-app/blob/63e9150ca17af4ed05880d4245e486481f73fcb4/vote/app.py) · [上游 Worker](https://github.com/dockersamples/example-voting-app/blob/63e9150ca17af4ed05880d4245e486481f73fcb4/worker/Program.cs)

在 Worker 初始化数据库后停下它，从上游 HTTP 入口为合成 ID `102432` 提交一次投票。使用 Redis `LRANGE` 在隔离队列中查找同一 `voter_id`，以及 PostgreSQL 只读会话中的 `SELECT count(*) FROM public.votes WHERE id = '102432'` 采集证据。恢复 Worker 后再采集一次。快照保留查询时间、结果和匹配数。

| 阶段 | Redis `queued_vote` | PostgreSQL `stored_vote` |
| --- | --- | --- |
| Worker 停止时提交投票 | 1 条，`observed` | 0 条，`not_observed` |
| Worker 恢复并处理后 | 0 条，`not_observed` | 1 条，`observed` |

`diagnose-voting` 接收每次采集的快照，适配器校验投票人 ID 和证据，Core 生成报告并区分零条匹配与查询不可用。首轮使用正整数测试 ID，接入时复用了既有诊断编排。投票人 ID 关联的是投票人的当前记录，报告中的独立消息 ID 留空。

复验使用投票页面响应中的 `voter_id` Cookie，应用生成了 `4a1c09a9dbcf77a`。随后用该 Cookie 提交投票，两次采集均按原值查找。Worker 停止时 Redis 和 PostgreSQL 分别匹配 1 条和 0 条，恢复后分别匹配 0 条和 1 条。两份报告保存了完整的字符串 ID。为支持这类目标，Core 的通用实体 ID 契约扩展为整数或字符串，诊断编排保持一致。HireHub 的整数投递 ID 和已有的整数投票快照可继续使用。

### 回放投票快照

对已运行的隔离投票示例，可以直接导出快照。传入示例的 Compose 项目名、所用的 Compose 文件、环境标签和页面分配的投票人 ID。若使用多个 Compose 文件，按启动时的顺序重复传入 `--compose-file`。

```powershell
python examples/capture_voting_snapshot.py --project-name voting-demo --compose-file PATH_TO_VOTING_COMPOSE --environment docker-voting-local --voter-id 4a1c09a9dbcf77a | Out-File -Encoding utf8 pending.json
```

导出使用示例容器自带的 `redis-cli` 和 `psql`。Redis 证据限于不超过 1000 条记录的隔离队列，超过上限会记为 `missing`。PostgreSQL 查询在只读会话内按 ID 统计。两个检查点分别采集并记录各自的观测时间，Worker 可能在两次读取之间处理投票。单个数据源超时或不可用时仍会采集另一侧。快照只保留匹配数和观测结果。另一次独立验收停止了 Redis，脚本仍导出了 PostgreSQL 中已入库的一条记录。 [Redis CLI](https://redis.io/docs/latest/develop/tools/cli/) · [psql](https://www.postgresql.org/docs/15/app-psql.html#APP-PSQL-INTERPOLATION)

快照为 UTF-8 JSON。应用分配的 `voter_id` 写成字符串，匹配保留大小写和前导零。整数字段仍可用于原有测试快照。时间使用带时区的 ISO 8601，按整秒、三位毫秒或六位微秒填写。每个检查点记录 `observed`、`timeout` 或 `missing`，其中 `observed` 携带非负整数 `row_count`。下面是一个可保存为 `pending.json` 的回放示例。恢复快照用实际复采时间和计数填写。示例将两次报告保存在专用的投票验收数据库。

```json
{
  "environment": "docker-voting-local",
  "voter_id": "4a1c09a9dbcf77a",
  "captured_at": "2026-11-22T05:30:01+00:00",
  "queued_vote": {"observed_at": "2026-11-22T05:30:00+00:00", "outcome": "observed", "row_count": 1},
  "stored_vote": {"observed_at": "2026-11-22T05:30:01+00:00", "outcome": "observed", "row_count": 0}
}
```

```powershell
python -m opspilot --db .opspilot/voting.sqlite3 diagnose-voting --snapshot pending.json --voter-id 4a1c09a9dbcf77a "投票是否已处理？"
python -m opspilot --db .opspilot/voting.sqlite3 report TASK_ID --format markdown
```

使用同一环境和投票人 ID 的两份报告生成复查对比。命令默认输出 JSON，加上 `--format markdown` 可生成表格。对比保留排队和入库状态、匹配数、查询结果及失败原因的变化。查询匹配数为 `0` 和查询超时各自保留原来的含义。

```powershell
python -m opspilot --db .opspilot/voting.sqlite3 compare-voting PENDING_TASK_ID STORED_TASK_ID --format markdown
```

日常复查可直接按环境和投票人 ID 选取最近保存的两份匹配报告。`reports` 可用同样的条件列出任务 ID，顺序从最新到最早。选择报告时按 ID 的原值匹配，环境标签也必须一致。

```powershell
python -m opspilot --db .opspilot/voting.sqlite3 reports --environment docker-voting-local --voter-id 4a1c09a9dbcf77a --limit 2
python -m opspilot --db .opspilot/voting.sqlite3 compare-voting-latest --environment docker-voting-local --voter-id 4a1c09a9dbcf77a --format markdown
```
