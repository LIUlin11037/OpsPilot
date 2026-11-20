"""Command-line entry point for OpsPilot."""

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

from opspilot.adapters.hirehub_capture import HireHubLiveAdapter
from opspilot.adapters.hirehub_snapshot import NOTIFICATION_QUEUE, HireHubSnapshotAdapter
from opspilot.adapters.http_processing import HttpProcessingAdapter
from opspilot.adapters.json_processing import JsonProcessingAdapter
from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.connectors.http_health import HttpHealthConnector
from opspilot.connectors.mysql import MySQLConnector
from opspilot.connectors.rabbitmq import RabbitMQQueueConnector
from opspilot.connectors.redis_health import RedisHealthConnector
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import TargetProcessingTool, run_diagnosis, run_queue_diagnosis
from opspilot.core.health_diagnosis import HealthTool, run_health_diagnosis
from opspilot.core.startup import run_startup
from opspilot.core.target_trace import run_target_trace
from opspilot.health_config import DEFAULT_HEALTH_TIMEOUT_SECONDS, HealthConfig, load_health_config
from opspilot.report_comparison import compare_health_reports, compare_trace_reports
from opspilot.report_markdown import render_markdown
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
    if args.queue is None:
        parser.error("--queue is required with --management-url")

    username = os.getenv("OPSPILOT_RABBITMQ_USER")
    password = os.getenv("OPSPILOT_RABBITMQ_PASSWORD")
    if username is None or password is None:
        parser.error("set OPSPILOT_RABBITMQ_USER and OPSPILOT_RABBITMQ_PASSWORD")

    return RabbitMQQueueConnector(
        management_url=args.management_url,
        queue=args.queue,
        username=username,
        password=password,
        vhost="/" if args.vhost is None else args.vhost,
        timeout_seconds=5.0 if args.timeout is None else args.timeout,
    )


def _target_processing_tool(
    args: argparse.Namespace, connector: RabbitMQQueueConnector
) -> TargetProcessingTool | None:
    if args.processing_record is not None:
        return JsonProcessingAdapter(
            args.processing_record, queue=connector.queue, vhost=connector.vhost
        )
    if args.processing_url is not None:
        return HttpProcessingAdapter(
            args.processing_url,
            queue=connector.queue,
            vhost=connector.vhost,
            timeout_seconds=connector.timeout_seconds,
        )
    return None


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

    username = os.getenv("OPSPILOT_MYSQL_USER")
    password = os.getenv("OPSPILOT_MYSQL_PASSWORD")
    if not username or password is None:
        parser.error("set OPSPILOT_MYSQL_USER and OPSPILOT_MYSQL_PASSWORD")

    queue = _rabbitmq_connector(args, parser)
    try:
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


def _require_report(database: Path, task_id: str, parser: argparse.ArgumentParser) -> StoredReport:
    report = load_report(database, task_id)
    if report is None:
        parser.exit(1, f"opspilot: no report found for task {task_id}\n")
    return report


def _health_config(args: argparse.Namespace) -> HealthConfig:
    if args.config is not None:
        if args.dependency_url or args.redis_dependency_url or args.timeout is not None:
            raise ValueError("--config takes dependency URLs and timeout from the configuration file.")
        return load_health_config(args.config)
    return HealthConfig(
        service_url=args.service_url,
        dependency_urls=tuple(args.dependency_url),
        redis_dependency_urls=tuple(args.redis_dependency_url),
        timeout_seconds=DEFAULT_HEALTH_TIMEOUT_SECONDS if args.timeout is None else args.timeout,
    )


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
        "--timeout", type=float, metavar="SECONDS", help="Socket timeout (default: 5)."
    )
    health.add_argument("question", help="Incident description.")
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
    report_comparisons = {
        "compare-hirehub": (
            commands.add_parser("compare-hirehub", help="Compare saved reports for one HireHub event."),
            compare_trace_reports,
        ),
        "compare-http": (
            commands.add_parser("compare-http", help="Compare saved HTTP health reports for one service."),
            compare_health_reports,
        ),
    }
    for comparison_parser, _ in report_comparisons.values():
        comparison_parser.add_argument("before_task_id", help="Task ID of the baseline report.")
        comparison_parser.add_argument("after_task_id", help="Task ID of the follow-up report.")
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
            summaries = list_reports(
                args.db, limit=args.limit, delivery_id=args.delivery_id, environment=environment
            )
            print(json.dumps(summaries, indent=2))
            return
        if args.command in report_comparisons:
            comparison_parser, compare_reports = report_comparisons[args.command]
            before = _require_report(args.db, args.before_task_id, parser)
            after = _require_report(args.db, args.after_task_id, parser)
            try:
                comparison = compare_reports(before, after)
            except ValueError as error:
                comparison_parser.error(str(error))
            print(json.dumps(comparison, indent=2))
            return
        if args.command == "report":
            stored_report = _require_report(args.db, args.task_id, parser)
        elif args.command == "diagnose-http":
            question = args.question.strip()
            if not question:
                health.error("question must not be empty")
            try:
                config = _health_config(args)
                service = HttpHealthConnector(config.service_url, config.timeout_seconds)
                dependencies: list[HealthTool] = [
                    HttpHealthConnector(url, config.timeout_seconds) for url in config.dependency_urls
                ]
                dependencies.extend(
                    RedisHealthConnector(url, config.timeout_seconds)
                    for url in config.redis_dependency_urls
                )
                report = run_health_diagnosis(question, service, dependencies)
            except ValueError as error:
                health.error(str(error))
            stored_report = save_report(args.db, report)
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
            stored_report = save_report(args.db, report)
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
                    processing_tool = _target_processing_tool(args, queue_tool)
                    report = run_queue_diagnosis(
                        question, queue_tool, processing_tool, message_id=args.message_id
                    )
                except ValueError as error:
                    diagnose.error(str(error))
            else:
                if has_processing_source:
                    diagnose.error("--processing-record and --processing-url require --management-url")
                if any(value is not None for value in (args.queue, args.vhost, args.timeout)):
                    diagnose.error("--queue, --vhost, and --timeout require --management-url")
                simulated_queue, adapter = scenarios[args.scenario]
                report = run_diagnosis(question, simulated_queue, adapter)
            stored_report = save_report(args.db, report)
    except (OSError, sqlite3.Error, json.JSONDecodeError) as error:
        parser.exit(1, f"opspilot: report storage error: {error}\n")

    if args.command == "report" and args.format == "markdown":
        sys.stdout.buffer.write(render_markdown(stored_report).encode("utf-8", "backslashreplace"))
        return
    print(json.dumps(stored_report, indent=2))


if __name__ == "__main__":
    main()
