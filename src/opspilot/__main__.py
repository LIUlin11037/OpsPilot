"""Command-line entry point for OpsPilot."""

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from functools import partial
from math import isfinite
from pathlib import Path
from typing import Literal

from opspilot.adapters.hirehub_capture import HireHubLiveAdapter
from opspilot.adapters.hirehub_snapshot import NOTIFICATION_QUEUE, HireHubSnapshotAdapter
from opspilot.adapters.http_processing import HttpProcessingAdapter
from opspilot.adapters.json_processing import JsonProcessingAdapter
from opspilot.adapters.order_trace import OrderTraceAdapter
from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.adapters.voting_snapshot import VotingSnapshotAdapter, parse_voter_id
from opspilot.connectors.http_health import HttpHealthConnector
from opspilot.connectors.jaeger import JaegerConnector
from opspilot.connectors.jsonl_service_log import JsonlServiceLogConnector
from opspilot.connectors.model_routing import model_from_environment
from opspilot.connectors.mysql import MySQLConnector, MySQLHealthConnector, mysql_health_endpoint
from opspilot.connectors.prometheus import PrometheusConnector
from opspilot.connectors.rabbitmq import RabbitMQQueueConnector, queue_endpoint
from opspilot.connectors.redis_health import RedisHealthConnector
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import (
    DiagnosisPlan,
    DiagnosisReport,
    DiagnosisRoute,
    Evidence,
    HealthEvidence,
    HealthTool,
    InvestigationBudget,
    PlannedCheck,
    ProcessingRead,
    RequestedCheck,
    TargetProcessingTool,
    read_health,
    run_diagnosis,
    run_queue_diagnosis,
)
from opspilot.core.health_diagnosis import MetricCheck, run_health_diagnosis
from opspilot.core.incident_report import summarize_incident
from opspilot.core.incident_traces import TRACE_FOLLOWUP_BUDGET_SECONDS, attach_incident_traces
from opspilot.core.incident_window import IncidentWindow, parse_incident_window
from opspilot.core.investigation import InvestigationTool, run_investigation, unavailable_read
from opspilot.core.model_routing import REQUEST_FIELDS, AdvisableWorkflow
from opspilot.core.request_trace import run_request_trace
from opspilot.core.startup import run_startup
from opspilot.core.target_trace import run_target_trace
from opspilot.core.trace_evidence import MAX_INCIDENT_TRACE_LOOKUPS, RequestTraceRead, normalize_trace_id
from opspilot.core.triage import select_diagnosis_route
from opspilot.handoff import export_reports, import_reports
from opspilot.health_config import DEFAULT_HEALTH_TIMEOUT_SECONDS, HealthConfig, load_health_config
from opspilot.incident_comparison import compare_incident_reports
from opspilot.investigation_store import (
    InvestigationBusyError, InvestigationRequest, TaskJournal, create_task, load_task, lock_task,
)
from opspilot.jaeger_config import MAX_RESPONSE_BYTES, MAX_SPANS, JaegerConfig
from opspilot.prometheus_config import PrometheusConfig
from opspilot.report_comparison import (
    compare_health_reports, compare_queue_reports, compare_trace_reports, compare_voting_reports,
)
from opspilot.report_markdown import (
    render_health_comparison_markdown,
    render_incident_comparison_markdown,
    render_markdown,
    render_queue_comparison_markdown,
    render_voting_comparison_markdown,
)
from opspilot.routing_evaluation import evaluate_routing, model_qualified
from opspilot.target_config import RabbitMQTargetConfig, TargetConfig, load_target_config
from opspilot.task_store import StoredReport, list_reports, load_report, save_report


_CONSUMER_HEALTH_WORKFLOW_REQUIRED = "--check-consumer-health requires the RabbitMQ queue workflow"
_VOTER_ID_ERROR = "--voter-id must contain 1 to 255 hexadecimal digits"


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
    check_consumer_health: bool = False,
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
        consumer_health_tool=consumer_health_tool, check_consumer_health=check_consumer_health,
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
    elif args.command == "compare-voting-latest":
        if not args.environment.strip():
            parser.error("--environment must not be empty")
        if parse_voter_id(args.voter_id) is None:
            parser.error(_VOTER_ID_ERROR)
        filters = {"environment": args.environment, "voter_id": args.voter_id}
        scope = "voting reports for the requested environment and voter ID"
    else:
        if not args.queue_source.strip():
            parser.error("--queue-source must not be empty")
        filters = {"queue_source": args.queue_source}
        scope = "queue reports for the requested queue source"
        if args.message_id is not None:
            if not args.message_id.strip():
                parser.error("--message-id must not be empty")
            filters["message_id"] = args.message_id
            scope += " and message ID"
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


def _validate_health_rechecks(
    config: HealthConfig, *, check_dependencies: bool, check_service_log: bool,
) -> None:
    if check_service_log and config.service_log_path is None:
        raise ValueError("--check-service-log requires service_log_path in the health configuration")
    if check_dependencies and not (
        config.dependency_urls or config.redis_dependency_urls or config.mysql_dependency_urls
    ):
        raise ValueError("--check-dependencies requires at least one configured HTTP, Redis, or MySQL dependency")


def _http_health_tool(url: str, timeout_seconds: float, field: str) -> HttpHealthConnector:
    try:
        return HttpHealthConnector(url, timeout_seconds)
    except ValueError as error:
        raise ValueError(f"{field}: {error}") from error


def _configured_metric_checks(
    metrics: PrometheusConfig, window: IncidentWindow | None = None,
) -> tuple[MetricCheck, ...]:
    connector = PrometheusConnector(metrics, os.getenv("OPSPILOT_PROMETHEUS_TOKEN"))
    return tuple(
        MetricCheck(metrics.endpoint, query.name, partial(connector.inspect, query, window=window))
        for query in metrics.queries
    )


def _run_configured_health(
    question: str, config: HealthConfig, *, check_dependencies: bool = False,
    check_service_log: bool = False,
    metrics: PrometheusConfig | None = None, check_metrics: bool = False,
) -> DiagnosisReport:
    _validate_health_rechecks(
        config, check_dependencies=check_dependencies, check_service_log=check_service_log,
    )
    service = _http_health_tool(config.service_url, config.timeout_seconds, "service_url")
    dependencies: list[HealthTool] = [
        _http_health_tool(url, config.timeout_seconds, f"dependency_urls[{index}]")
        for index, url in enumerate(config.dependency_urls)
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
    metric_checks = _configured_metric_checks(metrics) if metrics is not None else ()
    return run_health_diagnosis(
        question, service, dependencies, check_dependencies=check_dependencies,
        service_log=JsonlServiceLogConnector(config.service_log_path) if config.service_log_path is not None else None,
        check_service_log=check_service_log,
        metric_checks=metric_checks, check_metrics=check_metrics,
        metric_budget_seconds=metrics.timeout_seconds if metrics is not None else 5.0,
    )


def _run_incident_window(
    question: str, service: HealthTool, log_path: Path, metrics: PrometheusConfig, window: IncidentWindow,
    followup: JaegerConfig | None = None,
) -> DiagnosisReport:
    log = JsonlServiceLogConnector(log_path)
    report = run_health_diagnosis(
        question, service, service_log=lambda _: log.inspect_window(
            window, metrics.service, include_trace_candidates=followup is not None,
        ),
        check_service_log=True, metric_checks=_configured_metric_checks(metrics, window),
        check_metrics=True, metric_budget_seconds=metrics.timeout_seconds,
    )
    report = summarize_incident(report, window, metrics.service)
    if followup is None:
        return report
    token = os.getenv("OPSPILOT_JAEGER_TOKEN")

    def lookup(trace_id: str, remaining: float) -> RequestTraceRead:
        config = replace(followup, timeout_seconds=min(followup.timeout_seconds, remaining))
        return JaegerConnector(config, token).inspect_trace(trace_id)

    return attach_incident_traces(report, lookup)


def _investigation_request(target: TargetConfig, message_id: str | None) -> InvestigationRequest:
    health, rabbitmq = target.health, target.rabbitmq
    if health is None or rabbitmq is None:
        raise ValueError("The investigation requires health and rabbitmq sections in --config.")
    return {
        "service_url": health.service_url, "health_timeout_seconds": health.timeout_seconds,
        "management_url": rabbitmq.management_url, "queue": rabbitmq.queue, "vhost": rabbitmq.vhost,
        "queue_timeout_seconds": rabbitmq.timeout_seconds, "consumer_health_url": rabbitmq.consumer_health_url,
        "processing_url": rabbitmq.processing_url, "message_id": message_id,
        "processing_record": str(rabbitmq.processing_record) if rabbitmq.processing_record is not None else None,
    }


def _prepare_investigation(
    question: str, request: InvestigationRequest, budget: InvestigationBudget,
) -> tuple[DiagnosisPlan, Callable[..., DiagnosisReport]]:
    record_path = request["processing_record"]
    message_id = request["message_id"]
    rabbitmq = RabbitMQTargetConfig(
        request["management_url"], request["queue"], request["vhost"],
        timeout_seconds=request["queue_timeout_seconds"],
        consumer_health_url=request["consumer_health_url"], processing_url=request["processing_url"],
        processing_record=Path(record_path) if record_path is not None else None,
    )
    service = _http_health_tool(request["service_url"], request["health_timeout_seconds"], "health.service_url")
    endpoint = queue_endpoint(rabbitmq.management_url, rabbitmq.queue, rabbitmq.vhost)
    if endpoint.port == 0:
        raise ValueError("RabbitMQ management URL port must be positive.")
    queue_source = endpoint.geturl()
    checks: list[PlannedCheck] = [
        {"tool": "inspect_service_health", "when": "always", "count": 1, "sources": [service.endpoint]},
        {"tool": "inspect_queue", "when": "service_unhealthy_or_unknown", "count": 1, "sources": [queue_source]},
    ]
    scope: dict[str, str | int] = {
        "service_url": service.endpoint, "queue": rabbitmq.queue, "vhost": rabbitmq.vhost, "queue_source": queue_source,
    }

    def read_queue(remaining: float) -> Evidence:
        username = os.getenv("OPSPILOT_RABBITMQ_USER")
        password = os.getenv("OPSPILOT_RABBITMQ_PASSWORD")
        if not username or ":" in username or password is None:
            return unavailable_read("inspect_queue", queue_source, "credentials_unconfigured")
        return RabbitMQQueueConnector(
            rabbitmq.management_url, rabbitmq.queue, username, password, vhost=rabbitmq.vhost,
            timeout_seconds=min(rabbitmq.timeout_seconds, remaining),
        ).inspect_queue()

    def read_service(remaining: float) -> HealthEvidence:
        return {**read_health(replace(service, timeout_seconds=min(service.timeout_seconds, remaining))),
                "tool": "inspect_service_health"}

    consumer_tool = None
    if rabbitmq.consumer_health_url is not None:
        consumer = _http_health_tool(
            rabbitmq.consumer_health_url, rabbitmq.timeout_seconds, "rabbitmq.consumer_health_url",
        )

        def read_consumer(remaining: float) -> HealthEvidence:
            return {**read_health(replace(consumer, timeout_seconds=min(consumer.timeout_seconds, remaining))),
                    "tool": "inspect_consumer_endpoint"}

        consumer_tool = InvestigationTool(consumer.endpoint, read_consumer)
        checks.append({
            "tool": "inspect_consumer_endpoint", "when": "queue_activity_or_unavailable",
            "count": 1, "sources": [consumer.endpoint],
        })

    processing_tool = None
    if message_id is not None:
        scope["message_id"] = message_id
        if rabbitmq.processing_url is not None:
            adapter = HttpProcessingAdapter(
                rabbitmq.processing_url, rabbitmq.queue, rabbitmq.vhost, rabbitmq.timeout_seconds,
            )
            source = adapter.request_url(message_id=message_id).geturl()

            def read_processing(remaining: float) -> ProcessingRead:
                bounded = replace(adapter, timeout_seconds=min(adapter.timeout_seconds, remaining))
                return bounded.inspect_processing(message_id=message_id)

            processing_tool = InvestigationTool(source, read_processing)
        elif rabbitmq.processing_record is not None:
            record = JsonProcessingAdapter(rabbitmq.processing_record, queue=rabbitmq.queue, vhost=rabbitmq.vhost)
            processing_tool = InvestigationTool(
                record.path.absolute().as_uri(), lambda _: record.inspect_processing(message_id=message_id),
            )
        else:
            raise ValueError("--message-id requires rabbitmq.processing_url or rabbitmq.processing_record.")
        checks.append({
            "tool": "inspect_processing", "when": "exact_message_after_service_issue",
            "count": 1, "sources": [processing_tool.source],
        })

    plan: DiagnosisPlan = {
        "question": question, "routing": {"workflow": "investigation", "matched_terms": [], "selected_by": "workflow"},
        "scope": scope, "checks": checks, "budget": budget,
    }
    return plan, partial(
        run_investigation, question, InvestigationTool(service.endpoint, read_service),
        InvestigationTool(queue_source, read_queue), budget,
        consumer=consumer_tool, processing=processing_tool, message_id=message_id,
    )


def _run_saved_investigation(database: Path, task_id: str, *, announce: bool = False) -> StoredReport:
    if load_task(database, task_id) is None:
        raise ValueError(f"No investigation task found for {task_id}.")
    with lock_task(database, task_id):
        task = load_task(database, task_id)
        if task is None:
            raise ValueError(f"No investigation task found for {task_id}.")
        if task["status"] == "completed":
            stored = load_report(database, task_id)
            if stored is None:
                raise ValueError(f"Completed investigation {task_id} is missing its report.")
            return stored
        if announce:
            print(f"OpsPilot investigation task: {task_id}", file=sys.stderr, flush=True)
        plan = task["plan"]
        current, run = _prepare_investigation(plan["question"], task["request"], plan["budget"])
        if current != plan:
            raise ValueError("The current investigation workflow differs from the saved plan.")
        journal = TaskJournal(database, task)
        report = run(journal=journal)
        report["routing"] = plan["routing"]
        report["planned_scope"] = plan["scope"]
        report["planned_checks"] = plan["checks"]
        if task["previous_task_id"] is not None:
            report["previous_task_id"] = task["previous_task_id"]
        return journal.complete(report)


def _start_investigation(
    database: Path, request: InvestigationRequest, plan: DiagnosisPlan, *, previous_task_id: str | None = None,
) -> StoredReport:
    task = create_task(database, request, plan, previous_task_id=previous_task_id)
    return _run_saved_investigation(database, task["task_id"], announce=True)


def _metric_plan_check(
    metrics: PrometheusConfig, *, always: bool, window: IncidentWindow | None = None,
) -> PlannedCheck:
    check: PlannedCheck = {
        "tool": "inspect_service_metrics", "when": "always" if always else "service_unhealthy_or_unknown",
        "count": len(metrics.queries), "sources": [metrics.endpoint],
        "service": metrics.service,
        "queries": [
            {"name": query.name, "metric": query.metric, "unit": query.unit, "threshold": query.threshold}
            for query in metrics.queries
        ],
        "window_seconds": metrics.window_seconds, "step_seconds": metrics.step_seconds,
        "timeout_seconds": metrics.timeout_seconds,
    }
    if window is not None:
        check["window_seconds"] = window.duration_seconds
        check["step_seconds"] = window.metric_step(metrics.step_seconds)
        check["evaluation_start"] = window.evaluation_start.isoformat()
    return check


def _prepare_incident_window(
    args: argparse.Namespace, question: str,
) -> tuple[DiagnosisPlan, Callable[[], DiagnosisReport]]:
    window = parse_incident_window(args.incident_start, args.incident_end)
    if args.config is None:
        raise ValueError("An incident window requires --config.")
    other_workflow = args.workflow not in (None, "http_health") or any(option is not None for option in (
        args.message_id, args.delivery_id, args.voter_id, args.order_id,
        args.health_config, args.management_url, args.queue, args.vhost, args.timeout,
        args.max_steps, args.max_seconds, args.model_routing,
    ))
    if other_workflow or any((args.check_dependencies, args.check_service_log, args.check_metrics, args.check_consumer_health)):
        raise ValueError("An incident window selects its own HTTP, log and metric checks.")
    target = load_target_config(args.config)
    health, metrics = target.health, target.prometheus
    if health is None or health.service_log_path is None or metrics is None:
        raise ValueError("An incident window requires health.service_log_path and a prometheus section in --config.")
    followup = target.jaeger if args.follow_traces else None
    if args.follow_traces and followup is None:
        raise ValueError("--follow-traces requires a jaeger section in --config.")
    service = _http_health_tool(health.service_url, health.timeout_seconds, "service_url")
    plan: DiagnosisPlan = {
        "question": question,
        "routing": {"workflow": "http_health", "matched_terms": [], "selected_by": "incident_window"},
        "scope": {
            "service_url": service.endpoint, "service": metrics.service,
            "incident_start": window.start.isoformat(), "incident_end": window.end.isoformat(),
        },
        "checks": [
            {"tool": "inspect_service_health", "when": "always", "count": 1, "sources": [service.endpoint]},
            {
                "tool": "inspect_service_log", "when": "always", "count": 1,
                "sources": [health.service_log_path.as_uri()], "service": metrics.service,
            },
            _metric_plan_check(metrics, always=True, window=window),
        ],
    }

    if followup is not None:
        plan["checks"].append({
            "tool": "inspect_request_trace", "when": "scoped_error_trace_id", "count": MAX_INCIDENT_TRACE_LOOKUPS,
            "sources": [followup.base_url], "method": "GET",
            "timeout_seconds": min(followup.timeout_seconds, TRACE_FOLLOWUP_BUDGET_SECONDS),
            "total_timeout_seconds": TRACE_FOLLOWUP_BUDGET_SECONDS,
            "max_response_bytes": MAX_RESPONSE_BYTES, "max_spans": MAX_SPANS,
        })
    return plan, partial(_run_incident_window, question, service, health.service_log_path, metrics, window, followup)


def _prepare_request_trace(
    args: argparse.Namespace, question: str,
) -> tuple[DiagnosisPlan, Callable[[], DiagnosisReport]]:
    if any(value is not None for value in (
        args.workflow, args.incident_start, args.incident_end, args.health_config,
        args.management_url, args.queue, args.vhost, args.timeout, args.max_steps, args.max_seconds, args.model_routing,
    )) or any((args.check_dependencies, args.check_service_log, args.check_metrics, args.check_consumer_health,
              args.follow_traces)):
        raise ValueError("--trace-id cannot be combined with other workflow, scope, or check options.")
    trace_id = normalize_trace_id(args.trace_id)
    if args.config is None:
        raise ValueError("--trace-id requires --config with a jaeger section.")
    config = load_target_config(args.config).jaeger
    if config is None:
        raise ValueError("--trace-id requires a jaeger section in --config.")
    endpoint = JaegerConnector(config).endpoint(trace_id)
    plan: DiagnosisPlan = {
        "question": question,
        "routing": {"workflow": "request_trace", "matched_terms": [], "selected_by": "trace_id"},
        "scope": {"trace_id": trace_id, "trace_source": endpoint},
        "checks": [{
            "tool": "inspect_request_trace", "when": "always", "count": 1, "sources": [endpoint],
            "method": "GET", "timeout_seconds": config.timeout_seconds,
            "max_response_bytes": MAX_RESPONSE_BYTES, "max_spans": MAX_SPANS,
        }],
    }

    def diagnose_trace() -> DiagnosisReport:
        connector = JaegerConnector(config, os.getenv("OPSPILOT_JAEGER_TOKEN"))
        return run_request_trace(question, trace_id, connector.inspect_trace)

    return plan, diagnose_trace


def _prepare_auto_diagnosis(
    args: argparse.Namespace, parser: argparse.ArgumentParser,
) -> tuple[DiagnosisPlan, Callable[[], DiagnosisReport]]:
    question = args.question.strip()
    if not question:
        raise ValueError("question must not be empty")
    if args.trace_id is not None:
        return _prepare_request_trace(args, question)
    if args.incident_start is not None or args.incident_end is not None:
        return _prepare_incident_window(args, question)
    if args.follow_traces:
        raise ValueError("--follow-traces requires an incident window.")
    if args.message_id is not None:
        if not args.message_id.strip():
            raise ValueError("--message-id must not be empty")
        if args.config is None:
            raise ValueError("--message-id requires --config with a RabbitMQ processing source")
    if args.delivery_id is not None:
        if args.workflow is not None:
            raise ValueError("--delivery-id selects the snapshot workflow and cannot be combined with --workflow")
        if args.delivery_id <= 0:
            raise ValueError("--delivery-id must be positive")
        if args.config is None:
            raise ValueError("--delivery-id requires --config with a HireHub snapshot")
    voter_id = None
    if args.voter_id is not None:
        if args.workflow is not None:
            raise ValueError("--voter-id selects the snapshot workflow and cannot be combined with --workflow")
        voter_id = parse_voter_id(args.voter_id)
        if voter_id is None:
            raise ValueError(_VOTER_ID_ERROR)
        if args.config is None:
            raise ValueError("--voter-id requires --config with a voting snapshot")
    if args.order_id is not None:
        if not args.order_id.strip():
            raise ValueError("--order-id must not be empty")
        if args.workflow is not None:
            raise ValueError("--order-id selects the order trace and cannot be combined with --workflow")
        if args.config is None:
            raise ValueError("--order-id requires --config with an order section")
    direct_options = (args.health_config, args.management_url, args.queue, args.vhost, args.timeout)
    if args.config is not None and any(value is not None for value in direct_options):
        raise ValueError("--config cannot be combined with direct target options")
    if (args.management_url is None) != (args.queue is None):
        raise ValueError("--management-url and --queue must be supplied together")
    if args.management_url is None and (args.vhost is not None or args.timeout is not None):
        raise ValueError("--vhost and --timeout require --management-url")

    requested_check: RequestedCheck | None = None
    if args.check_metrics:
        requested_check = "check_metrics"
    elif args.check_service_log:
        requested_check = "check_service_log"
    elif args.check_dependencies:
        requested_check = "check_dependencies"
    health_option = f"--{requested_check.replace('_', '-')}" if requested_check is not None else None
    if args.check_consumer_health:
        if health_option is not None:
            raise ValueError(f"--check-consumer-health cannot be combined with {health_option}")
        requested_check = "check_consumer_health"

    if args.workflow == "investigation":
        if args.config is None:
            raise ValueError("--workflow investigation requires --config.")
        if requested_check is not None:
            raise ValueError("Investigation uses its own check conditions, without forced check options.")
        max_steps = 4 if args.max_steps is None else args.max_steps
        max_seconds = 15.0 if args.max_seconds is None else args.max_seconds
        if max_steps < 1:
            raise ValueError("--max-steps must be a positive integer.")
        if not isfinite(max_seconds) or max_seconds <= 0:
            raise ValueError("--max-seconds must be a finite positive number.")
        request = _investigation_request(load_target_config(args.config), args.message_id)
        plan, _ = _prepare_investigation(question, request, {"max_steps": max_steps, "max_seconds": max_seconds})
        return plan, partial(_start_investigation, args.db, request, plan)
    if args.max_steps is not None or args.max_seconds is not None:
        raise ValueError("--max-steps and --max-seconds require --workflow investigation.")

    if args.delivery_id is not None or voter_id is not None or args.order_id is not None:
        if health_option is not None:
            raise ValueError(f"{health_option} requires the HTTP health workflow")
        if args.check_consumer_health:
            raise ValueError(_CONSUMER_HEALTH_WORKFLOW_REQUIRED)
        target = load_target_config(args.config)
        if args.order_id is not None:
            order = target.order
            if order is None:
                raise ValueError("--order-id requires an order section in --config")
            adapter = OrderTraceAdapter(order.environment, order.orders_db, order.inventory_db, order.shipping_db)
            order_plan: DiagnosisPlan = {
                "question": question,
                "routing": {"workflow": "target_trace", "matched_terms": [], "selected_by": "entity_id"},
                "scope": {"entity_id": args.order_id, "environment": order.environment, **adapter.sources()},
                "checks": [{"tool": "inspect_target_trace", "when": "always", "count": 1}],
            }
            return order_plan, partial(run_target_trace, question, args.order_id, adapter)

        selected_by: Literal["delivery_id", "voter_id"]
        if voter_id is not None:
            snapshot = target.voting_snapshot
            if snapshot is None:
                raise ValueError("--voter-id requires voting.snapshot in --config")
            selected_by = "voter_id"
            entity_id = voter_id
            run_snapshot = partial(run_target_trace, question, voter_id, VotingSnapshotAdapter(snapshot))
        else:
            snapshot = target.hirehub_snapshot
            if snapshot is None:
                raise ValueError("--delivery-id requires hirehub.snapshot in --config")
            selected_by = "delivery_id"
            entity_id = args.delivery_id
            run_snapshot = partial(run_target_trace, question, args.delivery_id, HireHubSnapshotAdapter(snapshot))
        routing: DiagnosisRoute = {
            "workflow": "target_trace", "matched_terms": [], "selected_by": selected_by,
        }
        plan: DiagnosisPlan = {
            "question": question, "routing": routing,
            "scope": {selected_by: entity_id, "snapshot_source": snapshot.absolute().as_uri()},
            "checks": [{"tool": "inspect_target_trace", "when": "always", "count": 1}],
        }
        return plan, run_snapshot

    target = None
    try:
        routing = select_diagnosis_route(
            question, message_lookup=args.message_id is not None, workflow=args.workflow,
            requested_check=requested_check,
        )
    except ValueError as rule_error:
        if (
            args.model_routing is None or args.plan or args.workflow is not None
            or requested_check is not None or args.message_id is not None
        ):
            raise
        target = load_target_config(args.config) if args.config is not None else None
        candidates: list[AdvisableWorkflow] = []
        if args.health_config is not None or (target is not None and target.health is not None):
            candidates.append("http_health")
        if args.management_url is not None or (target is not None and target.rabbitmq is not None):
            candidates.append("queue")
        if not candidates:
            raise
        try:
            model = model_from_environment()
        except ValueError as error:
            raise ValueError(f"{rule_error} Model assistance unavailable. {error}") from error
        if not model_qualified(args.model_routing, model):
            raise ValueError(f"{rule_error} Model assistance requires a passing evaluation for this model.") from rule_error
        suggestion = model.suggest(question, tuple(candidates))
        if suggestion.workflow is None:
            raise ValueError(f"{rule_error} Model assistance returned {suggestion.reason}.") from rule_error
        routing = {
            "workflow": suggestion.workflow, "matched_terms": [], "selected_by": "qualified_model",
            "model": {
                "model": model.model, "request_data": list(REQUEST_FIELDS), "elapsed_ms": suggestion.elapsed_ms,
                "input_tokens": suggestion.input_tokens, "output_tokens": suggestion.output_tokens,
            },
        }
    if health_option is not None and routing["workflow"] != "http_health":
        raise ValueError(f"{health_option} requires the HTTP health workflow")
    if args.check_consumer_health and routing["workflow"] != "queue":
        raise ValueError(_CONSUMER_HEALTH_WORKFLOW_REQUIRED)
    if target is None and args.config is not None:
        target = load_target_config(args.config)
    if routing["workflow"] == "http_health":
        config = target.health if target is not None else None
        if args.health_config is not None:
            config = load_health_config(args.health_config)
        if config is None:
            raise ValueError(
                "Supply --health-config or a health section in --config for the HTTP health workflow."
            )
        _validate_health_rechecks(
            config, check_dependencies=args.check_dependencies, check_service_log=args.check_service_log,
        )
        service = _http_health_tool(config.service_url, config.timeout_seconds, "service_url")
        checks: list[PlannedCheck] = [{
            "tool": "inspect_service_health", "when": "always", "count": 1, "sources": [service.endpoint],
        }]
        if config.service_log_path is not None:
            checks.append({
                "tool": "inspect_service_log",
                "when": "always" if args.check_service_log else "service_unhealthy",
                "count": 1,
                "sources": [config.service_log_path.absolute().as_uri()],
            })
        dependency_sources = [
            _http_health_tool(url, config.timeout_seconds, f"dependency_urls[{index}]").endpoint
            for index, url in enumerate(config.dependency_urls)
        ]
        dependency_sources.extend(
            RedisHealthConnector(url, config.timeout_seconds).endpoint for url in config.redis_dependency_urls
        )
        for url in config.mysql_dependency_urls:
            mysql_health_endpoint(url)
            dependency_sources.append(url)
        if dependency_sources:
            dependency_check: PlannedCheck = {
                "tool": "inspect_dependency_health", "when": "service_unhealthy_or_unknown",
                "count": len(dependency_sources), "sources": dependency_sources,
            }
            if args.check_dependencies:
                dependency_check["when"] = "always"
            elif args.check_service_log:
                dependency_check["when"] = "service_unhealthy_or_unknown_or_log_errors"
            checks.append(dependency_check)
        metrics = target.prometheus if target is not None else None
        if args.check_metrics and metrics is None:
            raise ValueError("--check-metrics requires a prometheus section in --config")
        if metrics is not None:
            checks.append(_metric_plan_check(metrics, always=args.check_metrics))
        plan = {"question": question, "routing": routing, "scope": {"service_url": service.endpoint}, "checks": checks}
        return plan, partial(
            _run_configured_health, question, config,
            check_dependencies=args.check_dependencies, check_service_log=args.check_service_log,
            metrics=metrics, check_metrics=args.check_metrics,
        )

    rabbitmq = target.rabbitmq if target is not None else None
    if args.management_url is not None:
        rabbitmq = _rabbitmq_config(args, parser)
    if rabbitmq is None:
        raise ValueError(
            "Supply --management-url and --queue or a rabbitmq section in --config for the queue workflow."
        )
    if args.check_consumer_health and rabbitmq.consumer_health_url is None:
        raise ValueError("--check-consumer-health requires rabbitmq.consumer_health_url in --config")

    has_processing_source = rabbitmq.processing_url is not None or rabbitmq.processing_record is not None
    message_id = args.message_id
    if message_id is not None and not has_processing_source:
        raise ValueError("--message-id requires rabbitmq.processing_url or rabbitmq.processing_record")

    endpoint = queue_endpoint(rabbitmq.management_url, rabbitmq.queue, rabbitmq.vhost)
    scope: dict[str, str | int] = {"queue": rabbitmq.queue, "vhost": rabbitmq.vhost, "queue_source": endpoint.geturl()}
    if message_id is not None:
        scope["message_id"] = message_id
    checks = [{"tool": "inspect_queue", "when": "always", "count": 1, "sources": [endpoint.geturl()]}]
    if rabbitmq.consumer_health_url is not None:
        consumer = HttpHealthConnector(rabbitmq.consumer_health_url, rabbitmq.timeout_seconds)
        checks.append({
            "tool": "inspect_consumer_endpoint", "count": 1,
            "when": "always" if args.check_consumer_health else "queue_activity",
            "sources": [consumer.endpoint],
        })
    processing_source = None
    if rabbitmq.processing_record is not None:
        processing_source = rabbitmq.processing_record.absolute().as_uri()
    elif rabbitmq.processing_url is not None:
        processing = HttpProcessingAdapter(
            rabbitmq.processing_url, rabbitmq.queue, rabbitmq.vhost, rabbitmq.timeout_seconds,
        )
        processing_source = processing.request_url(message_id=message_id).geturl()
    if processing_source is not None:
        checks.append({
            "tool": "inspect_processing", "when": "always" if message_id is not None else "queue_activity",
            "count": 1, "sources": [processing_source],
        })

    def diagnose_queue() -> DiagnosisReport:
        return _run_configured_queue(
            question, _connect_rabbitmq(rabbitmq, parser),
            consumer_health_url=rabbitmq.consumer_health_url,
            processing_url=rabbitmq.processing_url,
            processing_record=rabbitmq.processing_record,
            message_id=message_id,
            check_consumer_health=args.check_consumer_health,
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
        "--timeout", type=float, metavar="SECONDS", help="Socket timeout (default: 5)."
    )
    health.add_argument("question", help="Incident description.")
    auto_diagnose = commands.add_parser(
        "diagnose-auto", help="Choose a configured HTTP, RabbitMQ, or business trace workflow.",
    )
    auto_diagnose.add_argument("--config", type=Path, help="JSON file with workflows for a target.")
    auto_diagnose.add_argument("--health-config", type=Path, help="JSON file with health endpoints.")
    auto_diagnose.add_argument("--management-url", help="RabbitMQ Management API base URL.")
    auto_diagnose.add_argument("--plan", action="store_true", help="Show the selected workflow and its configured checks.")
    auto_diagnose.add_argument("--check-metrics", action="store_true", help="Check configured service metrics explicitly.")
    auto_diagnose.add_argument("--incident-start", help="Start of the historical window, in ISO 8601 with a time zone.")
    auto_diagnose.add_argument("--incident-end", help="End of the historical window, in ISO 8601 with a time zone.")
    auto_diagnose.add_argument("--follow-traces", action="store_true", help="Follow scoped error-log Trace IDs in an incident window.")
    auto_diagnose.add_argument(
        "--workflow", choices=("http_health", "queue", "investigation"), help="Select a workflow explicitly.",
    )
    auto_diagnose.add_argument("--max-steps", type=int, help="Maximum investigation reads (default: 4).")
    auto_diagnose.add_argument("--max-seconds", type=float, help="Investigation time budget in seconds (default: 15).")
    auto_diagnose.add_argument(
        "--model-routing", type=Path, metavar="EVALUATION_JSON",
        help="Allow a qualified model to suggest a workflow when the rules need clarification.",
    )
    _add_queue_options(auto_diagnose)
    auto_scope = auto_diagnose.add_mutually_exclusive_group()
    auto_scope.add_argument("--message-id", help="Look up an exact message in the configured processing source.")
    auto_scope.add_argument("--delivery-id", type=int, help="Trace a delivery in the configured HireHub snapshot.")
    auto_scope.add_argument("--voter-id", help="Trace a voter in the configured voting snapshot.")
    auto_scope.add_argument("--order-id", help="Trace an exact order ID in the configured fulfillment databases.")
    auto_scope.add_argument("--trace-id", help="Read an exact Trace ID from the configured Jaeger endpoint.")
    auto_diagnose.add_argument("question", help="Incident description.")
    for health_parser in (health, auto_diagnose):
        health_parser.add_argument(
            "--check-dependencies", action="store_true",
            help="Check configured dependencies even when the service is healthy. Requires a configured dependency.",
        )
        health_parser.add_argument(
            "--check-service-log", action="store_true",
            help="Check the configured service log regardless of HTTP health.",
        )
    for queue_parser in (diagnose, auto_diagnose):
        queue_parser.add_argument(
            "--check-consumer-health", action="store_true",
            help="Check the configured consumer endpoint regardless of queue activity.",
        )
    voting = commands.add_parser("diagnose-voting", help="Trace a voter in an Example Voting App evidence export.")
    voting.add_argument("--snapshot", type=Path, required=True, help="Local voting evidence snapshot.")
    voting.add_argument("--voter-id", required=True, help="Voter ID from the snapshot, including hexadecimal IDs.")
    voting.add_argument("question", help="Incident description.")
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
    export_command = commands.add_parser("export-reports", help="Write selected saved reports to a local ZIP.")
    export_command.add_argument("--output", required=True, type=Path, help="Path for a new report ZIP.")
    export_command.add_argument("task_ids", nargs="+", help="Task IDs to include.")
    import_command = commands.add_parser("import-reports", help="Import a local report ZIP into SQLite.")
    import_command.add_argument("package", type=Path, help="Path to a report ZIP.")
    for name, help_text in (
        ("task", "Show a saved investigation and its attempts."),
        ("resume", "Continue an unfinished investigation."),
        ("recheck", "Start a fresh investigation using a completed task's plan."),
    ):
        commands.add_parser(name, help=help_text).add_argument("task_id")
    reports_command = commands.add_parser("reports", help="List recently saved reports.")
    reports_command.add_argument("--limit", type=int, default=20, help="Maximum reports (default: 20).")
    report_ids = reports_command.add_mutually_exclusive_group()
    report_ids.add_argument("--delivery-id", type=int, help="Match a HireHub delivery ID.")
    report_ids.add_argument("--voter-id", help="Match the exact Example Voting App voter ID.")
    report_ids.add_argument("--message-id", help="Match the exact message ID requested in a queue diagnosis.")
    reports_command.add_argument("--environment", help="Match the exact target environment label.")
    reports_command.add_argument("--service-url", help="Match the exact main service health URL.")
    reports_command.add_argument("--queue-source", help="Match the exact queue evidence source.")
    report_comparisons = {
        "compare-hirehub": commands.add_parser(
            "compare-hirehub", help="Compare saved reports for one HireHub event."
        ),
        "compare-voting": commands.add_parser(
            "compare-voting", help="Compare saved reports for one Example Voting App voter."
        ),
        "compare-voting-latest": commands.add_parser(
            "compare-voting-latest", help="Compare the two most recently saved reports for one voter."
        ),
        "compare-http": commands.add_parser(
            "compare-http", help="Compare saved HTTP health reports for one service."
        ),
        "compare-incident": commands.add_parser(
            "compare-incident", help="Compare two saved historical investigations for the same service."
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
    for command in ("compare-hirehub", "compare-voting", "compare-http", "compare-incident", "compare-queue"):
        comparison_parser = report_comparisons[command]
        comparison_parser.add_argument("before_task_id", help="Task ID of the baseline report.")
        comparison_parser.add_argument("after_task_id", help="Task ID of the follow-up report.")
    queue_comparisons = ("compare-queue", "compare-queue-latest")
    http_comparisons = ("compare-http", "compare-http-latest")
    voting_comparisons = ("compare-voting", "compare-voting-latest")
    for command in (*voting_comparisons, *http_comparisons, *queue_comparisons, "compare-incident"):
        report_comparisons[command].add_argument(
            "--format", choices=("json", "markdown"), default="json"
        )
    report_comparisons["compare-http-latest"].add_argument(
        "--service-url", required=True, help="Match the exact main service health URL."
    )
    report_comparisons["compare-queue-latest"].add_argument(
        "--queue-source", required=True, help="Match the exact queue evidence source."
    )
    report_comparisons["compare-queue-latest"].add_argument(
        "--message-id", help="Match the exact requested message ID and compare its processing observations."
    )
    report_comparisons["compare-voting-latest"].add_argument(
        "--voter-id", required=True, help="Match the exact Example Voting App voter ID."
    )
    report_comparisons["compare-voting-latest"].add_argument(
        "--environment", required=True, help="Match the exact target environment label."
    )
    for command in queue_comparisons:
        report_comparisons[command].add_argument(
            "--include-consumer-health", action="store_true",
            help="Include changes in saved consumer endpoint observations.",
        )
    report_comparisons["compare-queue"].add_argument(
        "--include-processing", action="store_true",
        help="Compare processing observations for the same explicitly requested message and source.",
    )
    for command in http_comparisons:
        report_comparisons[command].add_argument(
            "--include-service-log", action="store_true",
            help="Include changes in saved service log observations.",
        )
    inspect_queue = commands.add_parser(
        "inspect-queue", help="Read queue metrics from RabbitMQ Management."
    )
    inspect_queue.add_argument("--management-url", required=True)
    _add_queue_options(inspect_queue, require_queue=True)
    evaluate = commands.add_parser("evaluate-routing", help="Evaluate routing on the bundled synthetic cases.")
    evaluate.add_argument("--rules-only", action="store_true", help="Run the rule baseline without contacting a model.")
    args = parser.parse_args()

    if args.command is None:
        state = run_startup()
        print(f"OpsPilot is {state['status']}.")
        return

    if args.command == "evaluate-routing":
        try:
            model = None if args.rules_only else model_from_environment()
        except ValueError as error:
            evaluate.error(str(error))
        print(json.dumps(evaluate_routing(model), indent=2, ensure_ascii=True))
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

    if args.command in ("export-reports", "import-reports"):
        try:
            with _report_storage_errors(parser):
                if args.command == "export-reports":
                    task_ids = export_reports(args.db, args.output, args.task_ids)
                else:
                    task_ids = import_reports(args.package, args.db)
        except ValueError as error:
            parser.exit(1, f"opspilot: {error}\n")
        print(json.dumps({"count": len(task_ids), "task_ids": task_ids}, indent=2))
        return

    if args.command in ("task", "resume", "recheck"):
        try:
            with _report_storage_errors(parser):
                if args.command == "resume":
                    output = _run_saved_investigation(args.db, args.task_id)
                else:
                    task = load_task(args.db, args.task_id)
                    if task is None:
                        raise ValueError(f"No investigation task found for {args.task_id}.")
                    if args.command == "task":
                        output = task
                    else:
                        if task["status"] != "completed":
                            raise ValueError("An investigation must be completed before rechecking.")
                        output = _start_investigation(
                            args.db, task["request"], task["plan"], previous_task_id=task["task_id"],
                        )
        except (ValueError, InvestigationBusyError) as error:
            parser.exit(1, f"opspilot: {error}\n")
        print(json.dumps(output, indent=2))
        return

    try:
        if args.command == "reports":
            if not 0 < args.limit < 2**63:
                reports_command.error("--limit must be a positive 64-bit integer")
            if args.delivery_id is not None and args.delivery_id <= 0:
                reports_command.error("--delivery-id must be positive")
            if args.voter_id is not None and parse_voter_id(args.voter_id) is None:
                reports_command.error(_VOTER_ID_ERROR)
            if args.message_id is not None and not args.message_id.strip():
                reports_command.error("--message-id must not be empty")
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
                    service_url=args.service_url, queue_source=args.queue_source, voter_id=args.voter_id,
                    message_id=args.message_id,
                )
            print(json.dumps(summaries, indent=2))
            return
        if args.command in report_comparisons:
            comparison_parser = report_comparisons[args.command]
            if args.command in ("compare-http-latest", "compare-queue-latest", "compare-voting-latest"):
                before_id, after_id = _latest_comparison_task_ids(args, comparison_parser)
            else:
                before_id, after_id = args.before_task_id, args.after_task_id
            before = _require_report(args.db, before_id, parser)
            after = _require_report(args.db, after_id, parser)
            markdown = None
            try:
                if args.command == "compare-hirehub":
                    comparison = compare_trace_reports(before, after)
                elif args.command == "compare-incident":
                    comparison = compare_incident_reports(before, after)
                    if args.format == "markdown":
                        markdown = render_incident_comparison_markdown(comparison)
                elif args.command in voting_comparisons:
                    comparison = compare_voting_reports(before, after)
                    if args.format == "markdown":
                        markdown = render_voting_comparison_markdown(comparison)
                elif args.command in queue_comparisons:
                    comparison = compare_queue_reports(
                        before, after, include_consumer_health=args.include_consumer_health,
                        include_processing=(
                            args.include_processing if args.command == "compare-queue" else args.message_id is not None
                        ),
                    )
                    if args.format == "markdown":
                        markdown = render_queue_comparison_markdown(comparison)
                else:
                    comparison = compare_health_reports(
                        before, after, include_service_log=args.include_service_log,
                    )
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
                    check_service_log=args.check_service_log,
                )
            except ValueError as error:
                health.error(str(error))
        elif args.command == "diagnose-auto":
            try:
                plan, run_diagnostic = _prepare_auto_diagnosis(args, auto_diagnose)
                if args.plan:
                    print(json.dumps(plan, indent=2))
                    return
                if "budget" in plan:
                    with _report_storage_errors(parser):
                        stored = run_diagnostic()
                    print(json.dumps(stored, indent=2))
                    return
                report = run_diagnostic()
                report["routing"] = plan["routing"]
                report["planned_scope"] = plan["scope"]
                report["planned_checks"] = plan["checks"]
            except ValueError as error:
                auto_diagnose.error(str(error))
            except InvestigationBusyError as error:
                auto_diagnose.exit(1, f"opspilot: {error}\n")
        elif args.command == "diagnose-voting":
            question = args.question.strip()
            if not question:
                voting.error("question must not be empty")
            voter_id = parse_voter_id(args.voter_id)
            if voter_id is None:
                voting.error(_VOTER_ID_ERROR)
            report = run_target_trace(question, voter_id, VotingSnapshotAdapter(args.snapshot))
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
            if args.check_consumer_health and args.consumer_health_url is None:
                diagnose.error("--check-consumer-health requires --consumer-health-url")
            if args.management_url is not None:
                queue_tool = _rabbitmq_connector(args, diagnose)
                try:
                    report = _run_configured_queue(
                        question, queue_tool, message_id=args.message_id,
                        consumer_health_url=args.consumer_health_url,
                        processing_url=args.processing_url,
                        processing_record=args.processing_record,
                        check_consumer_health=args.check_consumer_health,
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
