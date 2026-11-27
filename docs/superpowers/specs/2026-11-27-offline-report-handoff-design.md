# OpsPilot v1.4 离线诊断交接包设计

## 目标与方案

值班工程师按任务 ID 选出需要交给同事的报告。接手人拿到一个 ZIP，解压即可阅读 Markdown，也可在自己的 OpsPilot 中导入，继续查看证据和对比。Traefik 故障报告、恢复报告及历史窗口报告提供真实验收样本。

| 方案 | 取舍 |
| --- | --- |
| ZIP，已选 | 同时携带可读报告、原始结构和完整性清单。接收时经过一次验证及导入。 |
| 精简 SQLite | 现有命令可直接读取。接手人通过 OpsPilot 查看 Markdown。 |

包的内容限定为所选的已保存报告及其 Markdown 渲染。证据中原有的来源地址、业务标识、时间窗、索引、诊断结论和任务关联随报告保存。目标配置、应用凭据、原始日志和现场脚本在包的内容范围之外。命令在本机读写。

## 使用方式

```text
python -m opspilot --db SOURCE.sqlite3 export-reports --output incident.zip TASK_ID [...]
python -m opspilot --db REVIEW.sqlite3 import-reports incident.zip
python -m opspilot --db REVIEW.sqlite3 report TASK_ID --format markdown
python -m opspilot --db REVIEW.sqlite3 compare-http FAULT_ID RECOVERY_ID --format markdown
```

导出只读源库，根据源记录顺序保存显式指定的任务。包内有版本化清单，以及每项报告的 JSON、Markdown 和 SHA-256 摘要。文件名使用包内编号，清单将编号对应到原任务 ID。已有同名文件时，提示用户换一个输出路径。

导入先核对清单版本、文件范围、大小、摘要和报告结构，再写入收件人的 SQLite。既有数据库中若有任一相同任务 ID，则整批返回冲突和对应 ID。新库在完整写入后发布，既有库用单个 SQLite 事务写入。导入后沿用 `report`、`reports` 和 `compare-*`，任务 ID 与来源报告保持一致。

## 验收与提交边界

1. 导出模块复用现有报告读取和 Markdown 渲染，从一个只读快照提取选定项。验证重复、未知 ID 和已有输出路径，检查失败后目标路径及源库状态。
2. 导入模块验证包的格式、内容完整性和数量及大小上限。验证未知版本、重复或额外文件、篡改内容和任务 ID 冲突，并用已有数据库检查整批写入的原子性。
3. 接入两个 CLI 命令，并用 Traefik 验收中的三份真实报告交接到独立目录。按原任务 ID 回读，检查当前 HTTP 504 到 200 的对比、两项指标及历史证据索引。复核接收库只包含所选任务，原库摘要保持一致。
4. 保存使用说明和实测记录。按可独立验证、回退的边界做本地提交，每笔提交前执行 `python -m opspilot` 和 `python -m pytest`。
