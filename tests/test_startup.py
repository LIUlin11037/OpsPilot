import subprocess
import sys


def test_module_entry_point_runs_startup_graph() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "opspilot"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.stdout == "OpsPilot is ready.\n"
