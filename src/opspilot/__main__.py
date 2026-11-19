"""Command-line entry point for OpsPilot."""

import argparse
import json

from opspilot.connectors.simulated import SimulatedQueueConnector
from opspilot.core.diagnosis import run_diagnosis
from opspilot.core.startup import run_startup


def main() -> None:
    scenarios = {
        "normal": SimulatedQueueConnector(pending_messages=0, consumer_running=True),
        "consumer-stopped": SimulatedQueueConnector(
            pending_messages=3, consumer_running=False
        ),
        "queue-timeout": SimulatedQueueConnector(
            pending_messages="timeout", consumer_running=True
        ),
        "consumer-data-missing": SimulatedQueueConnector(
            pending_messages=3, consumer_running="missing"
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

    report = run_diagnosis(question, scenarios[args.scenario])
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
