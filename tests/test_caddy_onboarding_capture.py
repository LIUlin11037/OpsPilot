import json
import runpy
from datetime import datetime, timedelta, timezone
from pathlib import Path


EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "caddy-onboarding"


def test_traffic_window_excludes_a_probe_from_the_same_second(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(EXAMPLE))
    capture = runpy.run_path(str(EXAMPLE / "acceptance.py"))["traffic"]
    start_second = datetime(2026, 11, 28, 2, 0, tzinfo=timezone.utc)

    class Clock:
        stamp = start_second + timedelta(milliseconds=700)

        @classmethod
        def now(cls, timezone):
            return cls.stamp

    def access(stamp):
        return {"logger": "http.log.access.log0", "ts": stamp.timestamp(), "status": 200}

    rows = [access(start_second + timedelta(milliseconds=100))]

    def request(url):
        rows.append(access(Clock.stamp))
        return 200, b"ok"

    def sleep(seconds):
        Clock.stamp += timedelta(seconds=seconds)

    monkeypatch.setitem(capture.__globals__, "datetime", Clock)
    monkeypatch.setitem(capture.__globals__, "WINDOW_SECONDS", 2)
    monkeypatch.setitem(capture.__globals__, "counter", lambda status: len(rows))
    monkeypatch.setitem(capture.__globals__, "get", request)
    monkeypatch.setitem(capture.__globals__, "sleep", sleep)
    monkeypatch.setitem(capture.__globals__, "docker", lambda *args: "\n".join(map(json.dumps, rows)))

    start, end, observed = capture(tmp_path, 200)
    assert start == start_second + timedelta(seconds=1)
    assert end == start + timedelta(seconds=2)
    assert observed == rows[1:]
