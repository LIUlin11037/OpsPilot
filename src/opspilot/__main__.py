"""Command-line entry point for OpsPilot."""

from opspilot.core.startup import run_startup


def main() -> None:
    state = run_startup()
    print(f"OpsPilot is {state['status']}.")


if __name__ == "__main__":
    main()
