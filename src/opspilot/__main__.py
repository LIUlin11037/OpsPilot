"""Command-line entry point for OpsPilot."""

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Literal, TypedDict

from opspilot.adapters.hirehub_capture import HireHubLiveAdapter
from opspilot.adapters.hirehub_snapshot import NOTIFICATION_QUEUE, HireHubSnapshotAdapter
from opspilot.adapters.http_processing import HttpProcessingAdapter
from opspilot.adapters.json_processing import JsonProcessingAdapter
from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.connectors.http_health import HttpHealthConnector
from opspilot.connectors.mysql import MySQLConnector, MySQLHealthConnector
from opspilot.connectors.rabbitmq import RabbitMQQueueConnector
from opspilot.connectors.redis_health import RedisHealthConnector
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import (
    DiagnosisReport,
    DiagnosisRoute,
    HealthTool,
    TargetProcessingTool,
    ToolName,
    run_diagnosis,
    run_queue_diagnosis,
)
from opspilot.core.health_diagnosis import run_health_diagnosis
from opspilot.core.startup import run_startup
from opspilot.core.target_trace import run_target_trace
from opspilot.core.triage import select_diagnosis_route
from opspilot.health_config import DEFAULT_HEALTH_TIMEOUT_SECONDS, HealthConfig, load_health_config
from opspilot.report_comparison import compare_health_reports, compare_queue_reports, compare_trace_reports
from opspilot.report_markdown import (
    render_health_comparison_markdown,
    render_markdown,
    render_queue_comparison_markdown,
)
from opspilot.target_config import RabbitMQTargetConfig, load_target_config
from opspilot.task_store import StoredReport, list_reports, load_report, save_report


def _add_queue_options(
    parser: argparse.ArgumentParser, *, require_queue: bool = False, fixed_queue: str | None = None
) -> None:
    if fixed_queue is None:
        parser.add_argument("--queue", required=require_queue)
    else:
        parser.set_defaults(queue=fixed_queue)
    parser.add_argument("--vhost", help="RabbitMQ virtual host (default: /).")
    parser.add_argument(
        "--timeout", type=float, metavar="SECONDS", help="Socket timeout (default: 5)."
    )


def _add_hirehub_live_options(parser: argparse.ArgumentParser, *, required: bool) -> None:
    parser.add_argument("--environment", required=required, help="Isolated environment label.")
    parser.add_argument("--recipient-id", type=int, required=required, help="Expected HR user ID.")
    parser.add_argument("--mysql-host", help="MySQL host (default: 127.0.0.1).")
    parser.add_argument("--mysql-port", type=int, help="MySQL port (default: 3306).")
    _add_queue_options(parser, fixed_queue=NOTIFICATION_QUEUE)


def _rabbitmq_connector(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> RabbitMQQueueConnector:
    return _connect_rabbitmq(_rabbitmq_config(args, parser), parser)


def _rabbitmq_config(args: argparse.Namespace, parser: argparse.ArgumentParser) -> RabbitMQTargetConfig:
    if args.queue is None:
        parser.error("--queue is required with --management-url")

    return RabbitMQTargetConfig(
        management_url=args.management_url,
        queue=args.queue,
        vhost="/" if args.vhost is None else args.vhost,
        timeout_seconds=5.0 if args.timeout is None else args.timeout,
    )


def _connect_rabbitmq(
    config: RabbitMQTargetConfig, parser: argparse.ArgumentParser,
) -> RabbitMQQueueConnector:
    username = os.getenv("OPSPILOT_RABBITMQ_USER")
    password = os.getenv("OPSPILOT_RABBITMQ_PASSWORD")
    if username is None or password is None:
        parser.error("set OPSPILOT_RABBITMQ_USER and OPSPILOT_RABBITMQ_PASSWORD")

    return RabbitMQQueueConnector(
        management_url=config.management_url,
        queue=config.queue,
        username=username,
        password=password,
        vhost=config.vhost,
        timeout_seconds=config.timeout_seconds,
    )


def _run_configured_queue(
    question: str,
    connector: RabbitMQQueueConnector,
    *,
    consumer_health_url: str | None = None,
    processing_url: str | None = None,
    processing_record: Path | None = None,
    message_id: str | None = None,
) -> DiagnosisReport:
    processing_tool: TargetProcessingTool | None = None
    if processing_record is not None:
        processing_tool = JsonProcessingAdapter(
            processing_record, queue=connector.queue, vhost=connector.vhost,
        )
    elif processing_url is not None:
        processing_tool = HttpProcessingAdapter(
            processing_url,
            queue=connector.queue,
            vhost=connector.vhost,
            timeout_seconds=connector.timeout_seconds,
        )
    consumer_health_tool = (
        HttpHealthConnector(consumer_health_url, connector.timeout_seconds)
        if consumer_health_url is not None else None
    )
    return run_queue_diagnosis(
        question, connector, processing_tool, message_id=message_id,
        consumer_health_tool=consumer_health_tool,
    )


def _mysql_credentials() -> tuple[str, str]:
    username = os.getenv("OPSPILOT_MYSQL_USER")
    password = os.getenv("OPSPILOT_MYSQL_PASSWORD")
    if not username or password is None:
        raise ValueError("set OPSPILOT_MYSQL_USER and OPSPILOT_MYSQL_PASSWORD")
    return username, password


def _hirehub_live_adapter(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> HireHubLiveAdapter:
    if args.environment is None or args.recipient_id is None:
        parser.error("--management-url requires --environment and --recipient-id")
    if args.delivery_id <= 0 or args.recipient_id <= 0:
        parser.error("--delivery-id and --recipient-id must be positive")
    environment = args.environment.strip()
    if not environment:
        parser.error("--environment must not be empty")

    try:
        username, password = _mysql_credentials()
        queue = _rabbitmq_connector(args, parser)
        mysql = MySQLConnector(
            host="127.0.0.1" if args.mysql_host is None else args.mysql_host,
            port=3306 if args.mysql_port is None else args.mysql_port,
            username=username,
            password=password,
            timeout_seconds=queue.timeout_seconds,
        )
    except ValueError as error:
        parser.error(str(error))
    return HireHubLiveAdapter(mysql, queue, environment, args.recipient_id)


@contextmanager
def _report_storage_errors(parser: argparse.ArgumentParser) -> Iterator[None]:
    try:
        yield
    except (OSError, sqlite3.Error, json.JSONDecodeError) as error:
        parser.exit(1, f"opspilot: report storage error: {error}\n")


def _require_report(database: Path, task_id: str, parser: argparse.ArgumentParser) -> StoredReport:
    with _report_storage_errors(parser):
        report = load_report(database, task_id)
    if report is None:
        parser.exit(1, f"opspilot: no report found for task {task_id}\n")
    return report


def _latest_comparison_task_ids(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> tuple[str, str]:
    if args.command == "compare-http-latest":
        if not args.service_url.strip():
            parser.error("--service-url must not be empty")
        filters = {"service_url": args.service_url}
        scope = "health reports for the requested service URL"
    else:
        if not args.queue_source.strip():
            parser.error("--queue-source must not be empty")
        filters = {"queue_source": args.queue_source}
        scope = "queue reports for the requested queue source"
    with _report_storage_errors(parser):
        reports = list_reports(args.db, limit=2, **filters)
    if len(reports) < 2:
        parser.exit(1, f"opspilot: fewer than two saved {scope}\n")
    follow_up, baseline = reports
    return baseline["task_id"], follow_up["task_id"]


def _health_config(args: argparse.Namespace) -> HealthConfig:
    if args.config is not None:
        if (
            args.dependency_url or args.redis_dependency_url
            or args.mysql_dependency_url or args.timeout is not None
        ):
            raise ValueError("--config takes dependency URLs and timeout from the configuration file.")
        return load_health_config(args.config)
    return HealthConfig(
        service_url=args.service_url,
        dependency_urls=tuple(args.dependency_url),
        redis_dependency_urls=tuple(args.redis_dependency_url),
        timeout_seconds=DEFAULT_HEALTH_TIMEOUT_SECONDS if args.timeout is None else args.timeout,
        mysql_dependency_urls=tuple(args.mysql_dependency_url),
    )


def _run_configured_health(
    question: str, config: HealthConfig, *, check_dependencies: bool = False,
) -> DiagnosisReport:
    service = HttpHealthConnector(config.service_url, config.timeout_seconds)
    dependencies: list[HealthTool] = [
        HttpHealthConnector(url, config.timeout_seconds) for url in config.dependency_urls
    ]
    dependencies.extend(
        RedisHealthConnector(url, config.timeout_seconds) for url in config.redis_dependency_urls
    )
    if config.mysql_dependency_urls:
        username, password = _mysql_credentials()
        dependencies.extend(
            MySQLHealthConnector(url, username, password, config.timeout_seconds)
            for url in config.mysql_dependency_urls
        )
    return run_health_diagnosis(
        question, service, dependencies, check_dependencies=check_dependencies,
    )


class _PlanCheck(TypedDict):
    tool: ToolName
    when: Literal["always", "service_unhealthy_or_unknown", "queue_activity"]
    count: int


class _AutoDiagnosisPlan(TypedDict):
    question: str
    routing: DiagnosisRoute
    scope: dict[str, str | int]
    checks: list[_PlanCheck]


def _prepare_auto_diagnosis(
    args: argparse.Namespace, parser: argparse.ArgumentParser,
) -> tuple[_AutoDiagnosisPlan, Callable[[], DiagnosisReport]]:
    question = args.question.strip()
    if not question:
        raise ValueError("question must not be empty")
    if args.message_id is not None:
        if not args.message_id.strip():
            raise ValueError("--message-id must not be empty")
        if args.config is None:
            raise ValueError("--message-id requires --config with a RabbitMQ processing source")
    if args.delivery_id is not None:
        if args.delivery_id <= 0:
            raise ValueError("--delivery-id must be positive")
        if args.config is None:
            raise ValueError("--delivery-id requires --config with a HireHub snapshot")
    direct_options = (args.health_config, args.management_url, args.queue, args.vhost, args.timeout)
    if args.config is not None and any(value is not None for value in direct_options):
        raise ValueError("--config cannot be combined with direct target options")
    if (args.management_url is None) != (args.queue is None):
        raise ValueError("--management-url and --queue must be supplied together")
    if args.management_url is None and (args.vhost is not None or args.timeout is not None):
        raise ValueError("--vhost and --timeout require --management-url")

    if args.delivery_id is not None:
        target = load_target_config(args.config)
        if target.hirehub_snapshot is None:
            raise ValueError("--delivery-id requires hirehub.snapshot in --config")
        routing: DiagnosisRoute = {
            "workflow": "target_trace", "matched_terms": [], "selected_by": "delivery_id",
        }
        plan: _AutoDiagnosisPlan = {
            "question": question, "routing": routing,
            "scope": {"delivery_id": args.delivery_id},
            "checks": [{"tool": "inspect_target_trace", "when": "always", "count": 1}],
        }
        return plan, partial(
            run_target_trace, question, args.delivery_id, HireHubSnapshotAdapter(target.hirehub_snapshot),
        )

    routing = select_diagnosis_route(question, message_lookup=args.message_id is not None)
    target = load_target_config(args.config) if args.config is not None else None
    if routing["workflow"] == "http_health":
        config = target.health if target is not None else None
        if args.health_config is not None:
            config = load_health_config(args.health_config)
        if config is None:
            raise ValueError(
                "HTTP health symptoms matched. Supply --health-config or a health section in --config."
            )
        checks: list[_PlanCheck] = [{"tool": "inspect_service_health", "when": "always", "count": 1}]
        dependency_count = (
            len(config.dependency_urls) + len(config.redis_dependency_urls) + len(config.mysql_dependency_urls)
        )
        if dependency_count:
            checks.append({
                "tool": "inspect_dependency_health", "when": "service_unhealthy_or_unknown",
                "count": dependency_count,
            })
        plan = {
            "question": question, "routing": routing, "scope": {}, "checks": checks,
        }
        return plan, partial(_run_configured_health, question, config)

    rabbitmq = target.rabbitmq if target is not None else None
    if args.management_url is not None:
        rabbitmq = _rabbitmq_config(args, parser)
    if rabbitmq is None:
        raise ValueError(
            "Queue symptoms matched. Supply --management-url and --queue or a rabbitmq section in --config."
        )

    has_processing_source = rabbitmq.processing_url is not None or rabbitmq.processing_record is not None
    message_id = args.message_id
    if message_id is not None and not has_processing_source:
        raise ValueError("--message-id requires rabbitmq.processing_url or rabbitmq.processing_record")

    scope: dict[str, str | int] = {"queue": rabbitmq.queue, "vhost": rabbitmq.vhost}
    if message_id is not None:
        scope["message_id"] = message_id
    checks = [{"tool": "inspect_queue", "when": "always", "count": 1}]
    if rabbitmq.consumer_health_url is not None:
        checks.append({"tool": "inspect_consumer_endpoint", "when": "queue_activity", "count": 1})
    if has_processing_source:
        checks.append({
            "tool": "inspect_processing", "when": "always" if message_id is not None else "queue_activity",
            "count": 1,
        })

    def diagnose_queue() -> DiagnosisReport:
        return _run_configured_queue(
            question, _connect_rabbitmq(rabbitmq, parser),
            consumer_health_url=rabbitmq.consumer_health_url,
            processing_url=rabbitmq.processing_url,
            processing_record=rabbitmq.processing_record,
            message_id=message_id,
        )

    return {"question": question, "routing": routing, "scope": scope, "checks": checks}, diagnose_queue


def main() -> None:
    no_processing_data = SimulatedProcessingAdapter()
    running_backlog = SimulatedQueueConnector(
        pending_messages=3,
        consumer_running=True,
        sampled_pending_message_ids=("msg-101", "msg-102"),
    )
    scenarios = {
        "normal": (
            SimulatedQueueConnector(pending_messages=0, consumer_running=True),
            no_processing_data,
        ),
        "consumer-stopped": (
            SimulatedQueueConnector(pending_messages=3, consumer_running=False),
            no_processing_data,
        ),
        "queue-timeout": (
            SimulatedQueueConnector(pending_messages="timeout", consumer_running=True),
            no_processing_data,
        ),
        "consumer-data-missing": (
            SimulatedQueueConnector(pending_messages=3, consumer_running="missing"),
            no_processing_data,
        ),
        "processing-failed": (
            running_backlog,
            SimulatedProcessingAdapter(latest_result="failed", message_id="msg-101"),
        ),
        "processing-failed-outside-sample": (
            running_backlog,
            SimulatedProcessingAdapter(latest_result="failed", message_id="msg-099"),
        ),
    }
    parser = argparse.ArgumentParser(prog="opspilot")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(".opspilot/reports.sqlite3"),
        metavar="PATH",
        help="SQLite report database (default: .opspilot/reports.sqlite3).",
    )
    commands = parser.add_subparsers(dest="command")
    diagnose = commands.add_parser("diagnose", help="Diagnose a queue and save a report.")
    source = diagnose.add_mutually_exclusive_group(required=True)
    source.add_argument("--scenario", choices=scenarios)
    source.add_argument("--management-url", help="RabbitMQ Management API base URL.")
    _add_queue_options(diagnose)
    processing_source = diagnose.add_mutually_exclusive_group()
    processing_source.add_argument(
        "--processing-record", type=Path, help="Read a target's JSON processing record."
    )
    processing_source.add_argument(
        "--processing-url", help="Read processing records from an HTTP target endpoint."
    )
    diagnose.add_argument("--consumer-health-url", help="Read consumer endpoint health on queue activity.")
    diagnose.add_argument("--message-id", help="Look up processing records for an exact ID.")
    diagnose.add_argument("question", help="Incident description.")
    health = commands.add_parser("diagnose-http", help="Diagnose a service through its health endpoint.")
    health_source = health.add_mutually_exclusive_group(required=True)
    health_source.add_argument("--service-url", help="Service health URL.")
    health_source.add_argument("--config", type=Path, help="JSON file with health endpoints and timeout.")
    health.add_argument(
        "--dependency-url", action="append", default=[], help="HTTP dependency health URL. Repeatable."
    )
    health.add_argument(
        "--redis-dependency-url", action="append", default=[], help="Redis dependency URL. Repeatable."
    )
    health.add_argument(
        "--mysql-dependency-url", action="append", default=[], help="MySQL dependency URL. Repeatable."
    )
    health.add_argument(
        "--check-dependencies", action="store_true",
        help="Check configured dependencies even when the service is healthy.",
    )
    health.add_argument(
        "--timeout", type=float, metavar="SECONDS", help="Socket timeout (default: 5)."
    )
    health.add_argument("question", help="Incident description.")
    auto_diagnose = commands.add_parser(
        "diagnose-auto", help="Choose a configured HTTP, RabbitMQ, or HireHub snapshot workflow.",
    )
    auto_diagnose.add_argument("--config", type=Path, help="JSON file with workflows for a target.")
    auto_diagnose.add_argument("--health-config", type=Path, help="JSON file with health endpoints.")
    auto_diagnose.add_argument("--management-url", help="RabbitMQ Management API base URL.")
    auto_diagnose.add_argument("--plan", action="store_true", help="Show the selected workflow and its configured checks.")
    _add_queue_options(auto_diagnose)
    auto_scope = auto_diagnose.add_mutually_exclusive_group()
    auto_scope.add_argument("--message-id", help="Look up an exact message in the configured processing source.")
    auto_scope.add_argument("--delivery-id", type=int, help="Trace a delivery in the configured HireHub snapshot.")
    auto_diagnose.add_argument("question", help="Incident description.")
    hirehub = commands.add_parser(
        "diagnose-hirehub", help="Diagnose a HireHub delivery with live reads or a snapshot."
    )
    hirehub_source = hirehub.add_mutually_exclusive_group(required=True)
    hirehub_source.add_argument("--snapshot", type=Path, help="Local HireHub evidence snapshot.")
    hirehub_source.add_argument("--management-url", help="RabbitMQ Management API base URL.")
    _add_hirehub_live_options(hirehub, required=False)
    hirehub.add_argument("--delivery-id", type=int, required=True)
    hirehub.add_argument("question", help="Incident description.")
    capture = commands.add_parser(
        "capture-hirehub", help="Export live HireHub observations as a read-only snapshot."
    )
    capture.add_argument("--delivery-id", type=int, required=True)
    capture.add_argument("--management-url", required=True, help="RabbitMQ Management API base URL.")
    _add_hirehub_live_options(capture, required=True)
    report_command = commands.add_parser("report", help="Show a saved report.")
    report_command.add_argument("task_id", help="Task ID returned by diagnose.")
    report_command.add_argument("--format", choices=("json", "markdown"), default="json")
    reports_command = commands.add_parser("reports", help="List recently saved reports.")
    reports_command.add_argument("--limit", type=int, default=20, help="Maximum reports (default: 20).")
    reports_command.add_argument("--delivery-id", type=int, help="Match a HireHub delivery ID.")
    reports_command.add_argument("--environment", help="Match a HireHub environment label.")
    reports_command.add_argument("--service-url", help="Match the exact main service health URL.")
    reports_command.add_argument("--queue-source", help="Match the exact queue evidence source.")
    report_comparisons = {
        "compare-hirehub": commands.add_parser(
            "compare-hirehub", help="Compare saved reports for one HireHub event."
        ),
        "compare-http": commands.add_parser(
            "compare-http", help="Compare saved HTTP health reports for one service."
        ),
        "compare-http-latest": commands.add_parser(
            "compare-http-latest", help="Compare the two most recently saved reports for one HTTP service."
        ),
        "compare-queue": commands.add_parser(
            "compare-queue", help="Compare queue observations in two saved diagnosis reports."
        ),
        "compare-queue-latest": commands.add_parser(
            "compare-queue-latest", help="Compare the two most recently saved reports for one queue."
        ),
    }
    for command in ("compare-hirehub", "compare-http", "compare-queue"):
        comparison_parser = report_comparisons[command]
        comparison_parser.add_argument("before_task_id", help="Task ID of the baseline report.")
        comparison_parser.add_argument("after_task_id", help="Task ID of the follow-up report.")
    queue_comparisons = ("compare-queue", "compare-queue-latest")
    for command in ("compare-http", "compare-http-latest", *queue_comparisons):
        report_comparisons[command].add_argument(
            "--format", choices=("json", "markdown"), default="json"
        )
    report_comparisons["compare-http-latest"].add_argument(
        "--service-url", required=True, help="Match the exact main service health URL."
    )
    report_comparisons["compare-queue-latest"].add_argument(
        "--queue-source", required=True, help="Match the exact queue evidence source."
    )
    for command in queue_comparisons:
        report_comparisons[command].add_argument(
            "--include-consumer-health", action="store_true",
            help="Include changes in saved consumer endpoint observations.",
        )
    inspect_queue = commands.add_parser(
        "inspect-queue", help="Read queue metrics from RabbitMQ Management."
    )
    inspect_queue.add_argument("--management-url", required=True)
    _add_queue_options(inspect_queue, require_queue=True)
    args = parser.parse_args()

    if args.command is None:
        state = run_startup()
        print(f"OpsPilot is {state['status']}.")
        return

    if args.command == "capture-hirehub":
        adapter = _hirehub_live_adapter(args, capture)
        try:
            snapshot = adapter.capture(args.delivery_id)
        except ValueError as error:
            capture.error(str(error))
        print(json.dumps(snapshot, indent=2))
        return

    if args.command == "inspect-queue":
        connector = _rabbitmq_connector(args, inspect_queue)
        try:
            evidence = connector.inspect_queue()
        except ValueError as error:
            inspect_queue.error(str(error))
        print(json.dumps(evidence, indent=2))
        if evidence["outcome"] != "observed":
            parser.exit(1)
        return

    try:
        if args.command == "reports":
            if not 0 < args.limit < 2**63:
                reports_command.error("--limit must be a positive 64-bit integer")
            if args.delivery_id is not None and args.delivery_id <= 0:
                reports_command.error("--delivery-id must be positive")
            environment = args.environment
            if environment is not None and not environment.strip():
                reports_command.error("--environment must not be empty")
            if args.service_url is not None and not args.service_url.strip():
                reports_command.error("--service-url must not be empty")
            if args.queue_source is not None and not args.queue_source.strip():
                reports_command.error("--queue-source must not be empty")
            with _report_storage_errors(parser):
                summaries = list_reports(
                    args.db, limit=args.limit, delivery_id=args.delivery_id, environment=environment,
                    service_url=args.service_url, queue_source=args.queue_source,
                )
            print(json.dumps(summaries, indent=2))
            return
        if args.command in report_comparisons:
            comparison_parser = report_comparisons[args.command]
            if args.command in ("compare-http-latest", "compare-queue-latest"):
                before_id, after_id = _latest_comparison_task_ids(args, comparison_parser)
            else:
                before_id, after_id = args.before_task_id, args.after_task_id
            before = _require_report(args.db, before_id, parser)
            after = _require_report(args.db, after_id, parser)
            markdown = None
            try:
                if args.command == "compare-hirehub":
                    comparison = compare_trace_reports(before, after)
                elif args.command in queue_comparisons:
                    comparison = compare_queue_reports(
                        before, after, include_consumer_health=args.include_consumer_health,
                    )
                    if args.format == "markdown":
                        markdown = render_queue_comparison_markdown(comparison)
                else:
                    comparison = compare_health_reports(before, after)
                    if args.format == "markdown":
                        markdown = render_health_comparison_markdown(comparison)
            except ValueError as error:
                comparison_parser.error(str(error))
            if markdown is not None:
                sys.stdout.buffer.write(markdown.encode("utf-8", "backslashreplace"))
            else:
                print(json.dumps(comparison, indent=2))
            return
        if args.command == "report":
            stored_report = _require_report(args.db, args.task_id, parser)
        elif args.command == "diagnose-http":
            question = args.question.strip()
            if not question:
                health.error("question must not be empty")
            try:
                report = _run_configured_health(
                    question, _health_config(args), check_dependencies=args.check_dependencies,
                )
            except ValueError as error:
                health.error(str(error))
        elif args.command == "diagnose-auto":
            try:
                plan, run_diagnostic = _prepare_auto_diagnosis(args, auto_diagnose)
                if args.plan:
                    print(json.dumps(plan, indent=2))
                    return
                report = run_diagnostic()
                report["routing"] = plan["routing"]
            except ValueError as error:
                auto_diagnose.error(str(error))
        elif args.command == "diagnose-hirehub":
            question = args.question.strip()
            if not question:
                hirehub.error("question must not be empty")
            if args.delivery_id <= 0:
                hirehub.error("--delivery-id must be positive")
            if args.snapshot is None:
                target = _hirehub_live_adapter(args, hirehub)
            else:
                if any(option is not None for option in (
                    args.environment, args.recipient_id, args.mysql_host,
                    args.mysql_port, args.vhost, args.timeout,
                )):
                    hirehub.error("live connection options require --management-url")
                target = HireHubSnapshotAdapter(args.snapshot)
            try:
                report = run_target_trace(question, args.delivery_id, target)
            except ValueError as error:
                hirehub.error(str(error))
        else:
            question = args.question.strip()
            if not question:
                diagnose.error("question must not be empty")
            has_processing_source = (
                args.processing_record is not None or args.processing_url is not None
            )
            if args.message_id is not None and not has_processing_source:
                diagnose.error("--message-id requires --processing-record or --processing-url")
            if args.management_url is not None:
                queue_tool = _rabbitmq_connector(args, diagnose)
                try:
                    report = _run_configured_queue(
                        question, queue_tool, message_id=args.message_id,
                        consumer_health_url=args.consumer_health_url,
                        processing_url=args.processing_url,
                        processing_record=args.processing_record,
                    )
                except ValueError as error:
                    diagnose.error(str(error))
            else:
                if has_processing_source:
                    diagnose.error("--processing-record and --processing-url require --management-url")
                if args.consumer_health_url is not None:
                    diagnose.error("--consumer-health-url requires --management-url")
                if any(value is not None for value in (args.queue, args.vhost, args.timeout)):
                    diagnose.error("--queue, --vhost, and --timeout require --management-url")
                simulated_queue, adapter = scenarios[args.scenario]
                report = run_diagnosis(question, simulated_queue, adapter)
    except OSError as error:
        parser.exit(1, f"opspilot: diagnosis error: {error}\n")

    if args.command != "report":
        with _report_storage_errors(parser):
            stored_report = save_report(args.db, report)

    if args.command == "report" and args.format == "markdown":
        sys.stdout.buffer.write(render_markdown(stored_report).encode("utf-8", "backslashreplace"))
        return
    print(json.dumps(stored_report, indent=2))


if __name__ == "__main__":
    main()
