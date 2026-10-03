"""Pytest root configuration and baseline test fixtures."""

import os
from collections.abc import Generator

import pytest


@pytest.fixture(autouse=True)
def setup_test_environment() -> Generator[None, None, None]:
    """Ensure mock AWS environment variables are set during test execution."""
    original_env = os.environ.copy()
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["ENVIRONMENT"] = "test"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

    yield

    os.environ.clear()
    os.environ.update(original_env)
