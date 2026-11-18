# OpsPilot

OpsPilot 面向异构微服务的故障诊断。本阶段提供一个命令行入口，用本地 LangGraph 节点验证启动链路。

在仓库根目录使用 Python 3.10 或更高版本运行以下命令（Windows PowerShell）。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m opspilot
.\.venv\Scripts\python.exe -m pytest
```

启动成功时输出 `OpsPilot is ready.`。安装依赖后，启动和测试均可离线运行。
