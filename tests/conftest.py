"""Pytest configuration and fixtures."""

import os
from pathlib import Path

import pytest

# Load .env file if it exists
try:
    from dotenv import load_dotenv

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ImportError:
    pass  # python-dotenv not installed


def has_openai() -> bool:
    """Check if OpenAI is available (module installed and API key set)."""
    try:
        import openai  # noqa: F401  # type: ignore[import-not-found]

        return bool(os.environ.get("OPENAI_API_KEY"))
    except ImportError:
        return False


def has_openai_sdk() -> bool:
    """Check if the OpenAI SDK is installed (no API key required)."""
    try:
        import openai  # noqa: F401  # type: ignore[import-not-found]

        return True
    except ImportError:
        return False


def has_litellm() -> bool:
    """Check if LiteLLM is available (module installed and OpenAI API key set).

    Note: Tests use gpt-4o-mini which requires OpenAI API key specifically.
    """
    try:
        import litellm  # noqa: F401  # type: ignore[import-not-found]

        # Tests use OpenAI models (gpt-4o-mini), so require OpenAI API key
        return bool(os.environ.get("OPENAI_API_KEY"))
    except ImportError:
        return False


# Marker for integration tests requiring OpenAI
requires_openai = pytest.mark.skipif(
    not has_openai(),
    reason="OpenAI not available (module not installed or OPENAI_API_KEY not set)",
)

# Marker for offline tests that only need the OpenAI SDK installed
requires_openai_sdk = pytest.mark.skipif(
    not has_openai_sdk(),
    reason="OpenAI SDK not installed",
)

# Marker for integration tests requiring LiteLLM (with OpenAI)
requires_litellm = pytest.mark.skipif(
    not has_litellm(),
    reason="LiteLLM not available (module not installed or OPENAI_API_KEY not set)",
)
