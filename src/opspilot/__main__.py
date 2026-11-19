"""Command-line entry point for OpsPilot."""

import argparse
import json

from opspilot.adapters.simulated import SimulatedProcessingAdapter
from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import run_diagnosis
from opspilot.core.startup import run_startup


def main() -> None:
    no_processing_data = SimulatedProcessingAdapter()
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
            SimulatedQueueConnector(pending_messages=3, consumer_running=True),
            SimulatedProcessingAdapter(latest_result="failed"),
        ),
    }
    parser = argparse.ArgumentParser(prog="opspilot")
    commands = parser.add_subparsers(dest="command")
    diagnose = commands.add_parser("diagnose", help="Diagnose a simulated queue.")
    diagnose.add_argument("--scenario", choices=scenarios, required=True)
    diagnose.add_argument("question", help="Incident description.")
    args = parser.parse_args()

    if args.command is None:
        state = run_startup()
        print(f"OpsPilot is {state['status']}.")
        return

    question = args.question.strip()
    if not question:
        diagnose.error("question must not be empty")

    connector, adapter = scenarios[args.scenario]
    report = run_diagnosis(question, connector, adapter)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
