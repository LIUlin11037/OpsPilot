import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--rabbitmq-live",
        action="store_true",
        help="Run acceptance tests against the configured test RabbitMQ broker.",
    )
