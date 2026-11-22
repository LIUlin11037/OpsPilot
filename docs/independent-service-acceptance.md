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
