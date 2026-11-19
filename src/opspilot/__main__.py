"""Command-line entry point for OpsPilot."""

import argparse
import json
import os
import sqlite3
from pathlib import Path

from opspilot.adapters.json_processing import JsonProcessingAdapter
from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.connectors.rabbitmq import RabbitMQQueueConnector
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import run_diagnosis, run_queue_diagnosis
from opspilot.core.startup import run_startup
from opspilot.task_store import load_report, save_report


def _add_queue_options(
    parser: argparse.ArgumentParser, *, require_queue: bool = False
) -> None:
    parser.add_argument("--queue", required=require_queue)
    parser.add_argument("--vhost", help="RabbitMQ virtual host (default: /).")
    parser.add_argument(
        "--timeout", type=float, metavar="SECONDS", help="Socket timeout (default: 5)."
    )


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
    diagnose.add_argument(
        "--processing-record", type=Path, help="Read a target's JSON processing record."
    )
    diagnose.add_argument("--message-id", help="Look up an exact ID in the processing record.")
    diagnose.add_argument("question", help="Incident description.")
    report_command = commands.add_parser("report", help="Show a saved report.")
    report_command.add_argument("task_id", help="Task ID returned by diagnose.")
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
        if args.command == "report":
            stored_report = load_report(args.db, args.task_id)
            if stored_report is None:
                parser.exit(1, f"opspilot: no report found for task {args.task_id}\n")
        else:
            question = args.question.strip()
            if not question:
                diagnose.error("question must not be empty")
            if args.message_id is not None and args.processing_record is None:
                diagnose.error("--message-id requires --processing-record")
            if args.management_url is not None:
                queue_tool = _rabbitmq_connector(args, diagnose)
                processing_tool = None
                if args.processing_record is not None:
                    processing_tool = JsonProcessingAdapter(
                        args.processing_record, queue=queue_tool.queue, vhost=queue_tool.vhost
                    )
                try:
                    report = run_queue_diagnosis(
                        question, queue_tool, processing_tool, message_id=args.message_id
                    )
                except ValueError as error:
                    diagnose.error(str(error))
            else:
                if args.processing_record is not None:
                    diagnose.error("--processing-record requires --management-url")
                if any(value is not None for value in (args.queue, args.vhost, args.timeout)):
                    diagnose.error("--queue, --vhost, and --timeout require --management-url")
                simulated_queue, adapter = scenarios[args.scenario]
                report = run_diagnosis(question, simulated_queue, adapter)
            stored_report = save_report(args.db, report)
    except (OSError, sqlite3.Error, json.JSONDecodeError) as error:
        parser.exit(1, f"opspilot: report storage error: {error}\n")

    print(json.dumps(stored_report, indent=2))


if __name__ == "__main__":
    main()
