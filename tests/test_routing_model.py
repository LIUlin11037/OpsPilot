import json
import os
import subprocess
import sys
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from time import sleep

import pytest

from opspilot.connectors.model_routing import RoutingModel
from opspilot.core.model_routing import validate_suggestion
from opspilot.routing_evaluation import CASES, evaluate_routing, model_qualified


@contextmanager
def _server(suggest, *, delay=0):
    posts, gets = [], []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            assert self.path == "/v1/chat/completions"
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            message = json.loads(request["messages"][1]["content"])
            posts.append(message)
            if delay:
                sleep(delay)
            content = suggest(message)
            response = json.dumps({
                "choices": [{"message": {"content": content}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5},
            }).encode()
            try:
                self.send_response(200)
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
            except ConnectionError:
                pass

        def do_GET(self):
            gets.append(self.path)
            self.send_response(204)
            self.end_headers()

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", posts, gets
        finally:
            server.shutdown()
            worker.join()


def _cli(directory, *args, endpoint=None, expected_code=0):
    env = {key: value for key, value in os.environ.items() if not key.startswith("OPSPILOT_ROUTER_")}
    if endpoint is not None:
        env.update(OPSPILOT_ROUTER_URL=endpoint, OPSPILOT_ROUTER_MODEL="fixture-model")
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", *args], cwd=directory, env=env,
        encoding="utf-8", capture_output=True, timeout=30,
    )
    assert result.returncode == expected_code, result.stderr
    return result


@pytest.mark.parametrize(("content", "reason"), [
    ('{"workflow":"queue"}', "out_of_scope"),
    ('{"workflow":"investigation"}', "out_of_scope"),
    ('{"workflow":"http_health","url":"https://invalid.example","max_steps":20}', "invalid_response"),
    ('{"workflow":null}', "abstained"),
    ('http_health', "invalid_response"),
])
def test_model_can_only_suggest_one_configured_workflow(content, reason):
    assert validate_suggestion(content, ("http_health",)) == (None, reason)


def test_model_timeouts_produce_a_local_fallback_reason():
    with _server(lambda _: '{"workflow":"http_health"}', delay=0.5) as (url, _, _):
        model = RoutingModel(f"{url}/v1/chat/completions", "fixture-model", timeout_seconds=0.08)
        advice = model.suggest("GET /checkout hangs", ("http_health",))
        assert advice.workflow is None and advice.reason == "model_timeout"


def test_remote_provider_without_its_own_key_is_unavailable():
    with pytest.raises(ValueError, match="requires OPSPILOT_ROUTER_API_KEY"):
        RoutingModel("https://router.invalid/v1/chat/completions", "fixture-model")


def test_rules_evaluation_offline_and_unqualified_without_a_model():
    report = evaluate_routing(None)
    assert report["case_count"] == len(CASES)
    assert report["calls"] == 0 and report["qualified"] is False
    assert report["rules"] == report["assisted"]


def test_cli_applies_only_a_qualified_configured_suggestion():
    labels = {case.question: case.expected for case in CASES}
    question = "The checkout request never returns a response."
    response = {"workflow": "http_health"}

    def suggest(message):
        asked = message["question"]
        return json.dumps(response if asked == question else {"workflow": labels[asked]})

    with TemporaryDirectory(prefix="opspilot-model-routing-") as name, _server(suggest) as (url, posts, gets):
        directory = Path(name)
        endpoint = f"{url}/v1/chat/completions"
        model = RoutingModel(endpoint, "fixture-model")
        assessment = evaluate_routing(model)
        assert assessment["qualified"] and assessment["assisted"]["wrong_routes"] == assessment["rules"]["wrong_routes"]
        assert assessment["calls"] and assessment["input_tokens"] == assessment["calls"] * 10
        (directory / "evaluation.json").write_text(json.dumps(assessment), encoding="utf-8")
        assert not model_qualified(directory / "evaluation.json", RoutingModel(endpoint, "another-model"))
        (directory / "target.json").write_text(json.dumps({
            "health": {"service_url": f"{url}/health"},
        }), encoding="utf-8")
        posts.clear()
        args = ("diagnose-auto", "--config", "target.json", "--model-routing", "evaluation.json")

        _cli(directory, *args, "--plan", question, endpoint=endpoint, expected_code=2)
        _cli(directory, *args, question, expected_code=2)
        assert posts == gets == []

        clear = json.loads(_cli(directory, *args, "HTTP health?", endpoint=endpoint).stdout)
        assert clear["routing"] == {"workflow": "http_health", "matched_terms": ["http", "health"]}
        assert posts == [] and gets == ["/health"]
        gets.clear()

        chosen = json.loads(_cli(directory, *args, question, endpoint=endpoint).stdout)
        assert chosen["routing"]["selected_by"] == "qualified_model"
        assert chosen["routing"]["model"]["request_data"] == ["question", "candidate_workflows"]
        assert chosen["planned_scope"] == {"service_url": f"{url}/health"}
        assert gets == ["/health"]
        assert len(posts) == 1 and set(posts[0]) == {"question", "candidate_workflows"}
        assert set(posts[0]["candidate_workflows"]) == {"http_health"}
        assert url not in json.dumps(posts[0])
        posts.clear()
        gets.clear()

        response["workflow"] = "queue"
        rejected = _cli(directory, *args, question, endpoint=endpoint, expected_code=2)
        assert "out_of_scope" in rejected.stderr and not gets and len(posts) == 1
        posts.clear()
        assessment["qualified"] = False
        (directory / "evaluation.json").write_text(json.dumps(assessment), encoding="utf-8")
        rejected = _cli(directory, *args, question, endpoint=endpoint, expected_code=2)
        assert "passing evaluation" in rejected.stderr and not posts and not gets
