# OpsPilot

OpsPilot 面向异构微服务的故障诊断。当前支持通过 CLI 提交「消息已入队但未消费」的模拟诊断任务。

在仓库根目录使用 Python 3.10 或更高版本运行以下命令（Windows PowerShell）。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m opspilot
.\.venv\Scripts\python.exe -m pytest
```

启动成功时输出 `OpsPilot is ready.`。安装依赖后，启动和测试均可离线运行。

## 模拟诊断

```powershell
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario consumer-stopped "消息已入队但未消费"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario normal "消息已入队但未消费"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario queue-timeout "消息已入队但未消费"
.\.venv\Scripts\python.exe -m opspilot diagnose --scenario consumer-data-missing "消息已入队但未消费"
```

`--scenario` 选择本地模拟状态。LangGraph 先查询队列，发现待消费消息时再查询消费者。`normal` 场景只返回队列证据，`consumer-stopped` 场景返回队列和消费者两条证据。

`queue-timeout` 场景记录队列查询超时并结束调查。`consumer-data-missing` 场景保留已观测到的积压，并记录消费者数据缺失。两种场景的诊断状态均为 `undetermined`。

JSON 报告包含用户提交的问题、诊断状态和结论。每条证据记录工具、来源、UTC 观察时间及结果类型 `outcome`。结果为 `observed` 时附带观测值。模拟场景直接返回 `timeout` 或 `missing`，便于离线复现。
