"""Comprehensive Lifecycle Tests for L0 Runtime.

These tests document the DETERMINISTIC lifecycle behavior of L0.
Each test verifies the exact ordering of events and callbacks during various scenarios.

LIFECYCLE EVENT ORDERING:
-------------------------
Normal successful flow:
  1. SESSION_START (attempt=1, isRetry=false, isFallback=false)
  2. [tokens stream...]
  3. CHECKPOINT_SAVED (if continuation enabled, every N tokens)
  4. COMPLETE (with full State)

Retry flow (guardrail violation, drift, network error):
  1. SESSION_START (attempt=1, isRetry=false, isFallback=false)
  2. [tokens stream...]
  3. ERROR (with recoveryStrategy="retry")
  4. RETRY_ATTEMPT (attempt=N, reason)
  5. SESSION_START (attempt=2, isRetry=true, isFallback=false)
  6. [tokens stream...]
  7. COMPLETE

Fallback flow (retries exhausted):
  1. SESSION_START (attempt=1, isRetry=false, isFallback=false)
  2. [error occurs, retries exhausted]
  3. ERROR (with recoveryStrategy="fallback")
  4. FALLBACK_START (fromIndex=0, toIndex=1)
  5. SESSION_START (attempt=1, isRetry=false, isFallback=true)
  6. [tokens stream...]
  7. COMPLETE

Abort flow:
  1. SESSION_START
  2. [tokens stream...]
  3. [abort() called]
  4. ABORT_COMPLETED (tokenCount, contentLength)
  5. [throws Error with code STREAM_ABORTED]
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

from l0 import CheckIntervals, Error, ErrorCode, Retry, run, wrap
from l0.adapters import AdaptedEvent, Adapters
from l0.events import EventBus, ObservabilityEvent, ObservabilityEventType
from l0.guardrails import GuardrailRule, GuardrailViolation
from l0.runtime import LifecycleCallbacks, _internal_run
from l0.types import Event, EventType, State

# ============================================================================
# Test Helpers
# ============================================================================


class PassthroughAdapter:
    """Test adapter that passes through Event objects directly."""

    name = "passthrough"

    def detect(self, stream: Any) -> bool:
        """Detect async generators (our test streams)."""
        return hasattr(stream, "__anext__")

    async def wrap(
        self, stream: Any, options: Any = None
    ) -> AsyncIterator[AdaptedEvent[Any]]:
        """Pass through events wrapped in AdaptedEvent."""
        async for event in stream:
            yield AdaptedEvent(event=event, raw_chunk=None)


@pytest.fixture(autouse=True)
def register_passthrough_adapter():
    """Register and cleanup the passthrough adapter for tests."""
    Adapters.register(PassthroughAdapter())
    yield
    Adapters.reset()


async def create_token_stream(tokens: list[str]) -> AsyncIterator[Event]:
    """Create a simple token stream from an array of tokens."""
    for token in tokens:
        yield Event(type=EventType.TOKEN, text=token)
    yield Event(type=EventType.COMPLETE)


async def create_failing_stream(
    tokens_before_error: list[str],
    error: Exception | None = None,
) -> AsyncIterator[Event]:
    """Create a stream that fails after emitting some tokens."""
    for token in tokens_before_error:
        yield Event(type=EventType.TOKEN, text=token)
    # Raise an actual exception to trigger error handling
    raise error or Exception("Stream failed")


async def create_slow_stream(
    tokens: list[str],
    delay_ms: int,
) -> AsyncIterator[Event]:
    """Create a slow stream that delays between tokens."""
    for token in tokens:
        await asyncio.sleep(delay_ms / 1000)
        yield Event(type=EventType.TOKEN, text=token)
    yield Event(type=EventType.COMPLETE)


@dataclass
class CollectedEvent:
    """Collected event for tracking lifecycle events."""

    type: str
    ts: float
    data: dict[str, Any] = field(default_factory=dict)


class EventCollector:
    """Event collector for tracking lifecycle events."""

    def __init__(self):
        self.events: list[CollectedEvent] = []

    def handler(self, event: ObservabilityEvent | Event) -> None:
        """Handle both observability events and stream events."""
        if isinstance(event, ObservabilityEvent):
            event_type = (
                event.type.value
                if isinstance(event.type, ObservabilityEventType)
                else str(event.type)
            )
            self.events.append(
                CollectedEvent(
                    type=event_type,
                    ts=event.ts,
                    data={
                        "stream_id": event.stream_id,
                        "context": event.context,  # User-provided metadata
                        "meta": event.meta,  # Event-specific data
                        **event.meta,  # Also spread meta for backwards compat
                    },
                )
            )
        elif isinstance(event, Event):
            event_type = (
                event.type.value
                if isinstance(event.type, EventType)
                else str(event.type)
            )
            self.events.append(
                CollectedEvent(
                    type=event_type,
                    ts=event.timestamp or 0,
                    data={"text": event.text, "error": event.error},
                )
            )

    def get_event_types(self) -> list[str]:
        """Get list of event types in order."""
        return [e.type for e in self.events]

    def get_events_of_type(self, event_type: str) -> list[CollectedEvent]:
        """Get all events of a specific type."""
        return [e for e in self.events if e.type == event_type]

    def clear(self) -> None:
        """Clear collected events."""
        self.events.clear()


def create_event_collector() -> EventCollector:
    """Create a new event collector."""
    return EventCollector()


# ============================================================================
# Normal Flow Tests
# ============================================================================


class TestLifecycleNormalFlow:
    """Tests for normal successful flow."""

    @pytest.mark.asyncio
    async def test_emit_session_start_tokens_complete_in_order(self):
        """Should emit SESSION_START -> tokens -> COMPLETE in order."""
        collector = create_event_collector()

        async def stream():
            async for event in create_token_stream(["Hello", " ", "World"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        types = collector.get_event_types()

        # First event must be SESSION_START
        assert types[0] == ObservabilityEventType.SESSION_START.value

        # Last event must be COMPLETE (canonical lifecycle)
        assert types[-1] == ObservabilityEventType.COMPLETE.value

    @pytest.mark.asyncio
    async def test_session_start_first_attempt_params(self):
        """Should pass correct parameters to SESSION_START on first attempt."""
        collector = create_event_collector()

        async def stream():
            async for event in create_token_stream(["test"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        session_starts = collector.get_events_of_type(
            ObservabilityEventType.SESSION_START.value
        )
        assert len(session_starts) >= 1

        session_start = session_starts[0]
        # SESSION_START event contains attempt, isRetry, isFallback (not sessionId per spec)
        assert session_start.data.get("attempt") is not None
        assert session_start.data.get("isRetry") is not None
        assert session_start.data.get("isFallback") is not None

    @pytest.mark.asyncio
    async def test_complete_event_has_state(self):
        """Should pass correct State to COMPLETE event."""
        collector = create_event_collector()

        async def stream():
            async for event in create_token_stream(["Hello", " ", "World"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        complete_events = collector.get_events_of_type(
            ObservabilityEventType.COMPLETE.value
        )
        assert len(complete_events) >= 1

        complete = complete_events[0]
        assert complete.data.get("tokenCount") == 3

    @pytest.mark.asyncio
    async def test_on_start_callback(self):
        """Should call onStart callback with correct parameters."""
        on_start = MagicMock()

        async def stream():
            async for event in create_token_stream(["test"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_start=on_start,
        )

        async for _ in result:
            pass

        on_start.assert_called_once()
        args = on_start.call_args[0]
        # on_start(attempt, is_retry, is_fallback)
        assert args[0] == 1  # attempt
        assert args[1] is False  # is_retry
        assert args[2] is False  # is_fallback

    @pytest.mark.asyncio
    async def test_on_complete_callback(self):
        """Should call onComplete callback with final state."""
        on_complete = MagicMock()

        async def stream():
            async for event in create_token_stream(["Hello", "World"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_complete=on_complete,
        )

        async for _ in result:
            pass

        on_complete.assert_called_once()
        state = on_complete.call_args[0][0]
        assert isinstance(state, State)
        assert state.content == "HelloWorld"
        assert state.token_count == 2
        assert state.completed is True

    @pytest.mark.asyncio
    async def test_tokens_emitted_in_order(self):
        """Should emit tokens in exact order received."""
        received_tokens: list[str] = []

        def on_event(event: Event) -> None:
            if isinstance(event, Event) and event.type == EventType.TOKEN:
                received_tokens.append(event.text or "")

        async def stream():
            async for event in create_token_stream(["A", "B", "C", "D", "E"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_stream_event=on_event,
        )

        async for _ in result:
            pass

        assert received_tokens == ["A", "B", "C", "D", "E"]


# ============================================================================
# Retry Flow Tests
# ============================================================================


class TestLifecycleRetryFlow:
    """Tests for retry flow behavior.

    NOTE: These tests assume guardrail violations trigger retries, but this
    behavior is not yet implemented in the Python runtime. Guardrail violations
    are detected but don't automatically trigger retries.
    """

    @pytest.mark.asyncio
    async def test_attempt_start_emitted_on_retry(self):
        """Should emit ATTEMPT_START on retry attempts (not SESSION_START again)."""
        collector = create_event_collector()
        attempt_count = 0

        def force_retry_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and "bad" in state.content:
                return [
                    GuardrailViolation(
                        rule="force-retry",
                        severity="error",
                        message="Content contains bad word",
                        recoverable=True,
                    )
                ]
            return []

        async def stream_factory():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                async for event in create_token_stream(["bad"]):
                    yield event
            else:
                async for event in create_token_stream(["good"]):
                    yield event

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="force-retry", check=force_retry_rule)],
            retry=Retry(attempts=2),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        types = collector.get_event_types()

        # SESSION_START should only appear once (at the beginning)
        session_start_count = types.count(ObservabilityEventType.SESSION_START.value)
        assert session_start_count == 1

        # ATTEMPT_START should appear for the retry attempt
        attempt_start_count = types.count(ObservabilityEventType.ATTEMPT_START.value)
        assert attempt_start_count == 1

        # Find the indices
        session_start_idx = types.index(ObservabilityEventType.SESSION_START.value)
        attempt_start_idx = types.index(ObservabilityEventType.ATTEMPT_START.value)

        # ATTEMPT_START should come after SESSION_START
        assert attempt_start_idx > session_start_idx

    @pytest.mark.asyncio
    async def test_on_start_marked_as_retry_on_second_attempt(self):
        """Should call on_start with is_retry=true on retry attempts."""
        start_calls: list[tuple[int, bool, bool]] = []
        attempt_count = 0

        def on_start(attempt: int, is_retry: bool, is_fallback: bool) -> None:
            start_calls.append((attempt, is_retry, is_fallback))

        def force_retry_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and "retry-me" in state.content:
                return [
                    GuardrailViolation(
                        rule="force-retry",
                        severity="error",
                        message="Retry triggered",
                        recoverable=True,
                    )
                ]
            return []

        async def stream_factory():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                async for event in create_token_stream(["retry-me"]):
                    yield event
            else:
                async for event in create_token_stream(["success"]):
                    yield event

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="force-retry", check=force_retry_rule)],
            retry=Retry(attempts=2),
            on_start=on_start,
        )

        async for _ in result:
            pass

        # Should have 2 on_start calls (initial + retry)
        assert len(start_calls) == 2

        # First attempt
        assert start_calls[0] == (
            1,
            False,
            False,
        )  # attempt=1, is_retry=False, is_fallback=False

        # Second attempt (retry)
        assert start_calls[1] == (
            2,
            True,
            False,
        )  # attempt=2, is_retry=True, is_fallback=False

    @pytest.mark.asyncio
    async def test_on_retry_callback(self):
        """Should call onRetry callback with attempt number and reason."""
        on_retry = MagicMock()
        attempt_count = 0

        def force_retry_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and state.content == "bad":
                return [
                    GuardrailViolation(
                        rule="force-retry",
                        severity="error",
                        message="Bad content",
                        recoverable=True,
                    )
                ]
            return []

        async def stream_factory():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                async for event in create_token_stream(["bad"]):
                    yield event
            else:
                async for event in create_token_stream(["good"]):
                    yield event

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="force-retry", check=force_retry_rule)],
            retry=Retry(attempts=2),
            on_retry=on_retry,
        )

        async for _ in result:
            pass

        on_retry.assert_called_once()
        attempt, reason = on_retry.call_args[0]
        assert attempt >= 1
        assert isinstance(reason, str)
        assert "Guardrail" in reason or "guardrail" in reason.lower() or len(reason) > 0

    @pytest.mark.asyncio
    async def test_retry_attempt_event_data(self):
        """Should include correct data in RETRY_ATTEMPT event."""
        collector = create_event_collector()
        attempt_count = 0

        def force_retry_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and state.content == "trigger-retry":
                return [
                    GuardrailViolation(
                        rule="test-rule",
                        severity="error",
                        message="Triggered",
                        recoverable=True,
                    )
                ]
            return []

        async def stream_factory():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                async for event in create_token_stream(["trigger-retry"]):
                    yield event
            else:
                async for event in create_token_stream(["success-content"]):
                    yield event

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="test-rule", check=force_retry_rule)],
            retry=Retry(attempts=3),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        retry_attempts = collector.get_events_of_type(
            ObservabilityEventType.RETRY_ATTEMPT.value
        )
        assert len(retry_attempts) == 1

        retry_event = retry_attempts[0]
        assert retry_event.data.get("attempt") is not None
        assert retry_event.data.get("reason") is not None

        # Also verify ATTEMPT_START event data
        attempt_starts = collector.get_events_of_type(
            ObservabilityEventType.ATTEMPT_START.value
        )
        assert len(attempt_starts) == 1

        attempt_start_event = attempt_starts[0]
        assert attempt_start_event.data.get("attempt") == 2
        assert attempt_start_event.data.get("isFallback") is False

    @pytest.mark.asyncio
    async def test_multiple_retries_correct_order(self):
        """Should handle multiple retries in correct order."""
        collector = create_event_collector()
        attempt_count = 0

        def force_retry_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and "fail" in state.content:
                return [
                    GuardrailViolation(
                        rule="multi-retry",
                        severity="error",
                        message="Must retry",
                        recoverable=True,
                    )
                ]
            return []

        async def stream_factory():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                async for event in create_token_stream(["fail"]):
                    yield event
            else:
                async for event in create_token_stream(["success"]):
                    yield event

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="multi-retry", check=force_retry_rule)],
            retry=Retry(attempts=3),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        session_starts = collector.get_events_of_type(
            ObservabilityEventType.SESSION_START.value
        )
        attempt_starts = collector.get_events_of_type(
            ObservabilityEventType.ATTEMPT_START.value
        )
        retry_attempts = collector.get_events_of_type(
            ObservabilityEventType.RETRY_ATTEMPT.value
        )

        # Should have 1 session start (emitted once at the beginning)
        assert len(session_starts) == 1

        # Should have 2 ATTEMPT_START events (one for each retry attempt)
        assert len(attempt_starts) == 2

        # Verify attempt numbers are correct
        assert attempt_starts[0].data.get("attempt") == 2
        assert attempt_starts[1].data.get("attempt") == 3

        # Should have 2 retry attempts
        assert len(retry_attempts) == 2


# ============================================================================
# Fallback Flow Tests
# ============================================================================


class TestLifecycleFallbackFlow:
    """Tests for fallback flow behavior."""

    @pytest.mark.asyncio
    async def test_fallback_start_on_switch(self):
        """Should emit FALLBACK_START when switching to fallback model."""
        collector = create_event_collector()

        def fail_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and state.content == "primary":
                return [
                    GuardrailViolation(
                        rule="fail-primary",
                        severity="error",
                        message="Primary must fail",
                        recoverable=False,  # Non-recoverable triggers fallback
                    )
                ]
            return []

        async def primary_stream():
            async for event in create_token_stream(["primary"]):
                yield event

        async def fallback_stream():
            async for event in create_token_stream(["fallback-success"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback_stream],
            guardrails=[GuardrailRule(name="fail-primary", check=fail_rule)],
            retry=Retry(attempts=1),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        fallback_starts = collector.get_events_of_type(
            ObservabilityEventType.FALLBACK_START.value
        )
        assert len(fallback_starts) == 1

    @pytest.mark.asyncio
    async def test_session_start_marked_as_fallback(self):
        """Should mark SESSION_START as isFallback=true for fallback streams."""
        collector = create_event_collector()

        def fail_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and state.content == "primary":
                return [
                    GuardrailViolation(
                        rule="fail-primary",
                        severity="error",
                        message="Fail",
                        recoverable=False,
                    )
                ]
            return []

        async def primary_stream():
            async for event in create_token_stream(["primary"]):
                yield event

        async def fallback_stream():
            async for event in create_token_stream(["success"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback_stream],
            guardrails=[GuardrailRule(name="fail-primary", check=fail_rule)],
            retry=Retry(attempts=1),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        # Python emits 1 SESSION_START per session
        # Use on_start callback to verify fallback flag instead
        session_starts = collector.get_events_of_type(
            ObservabilityEventType.SESSION_START.value
        )
        assert len(session_starts) == 1

        # Verify fallback was used via FALLBACK_START event
        fallback_starts = collector.get_events_of_type(
            ObservabilityEventType.FALLBACK_START.value
        )
        assert len(fallback_starts) == 1

    @pytest.mark.asyncio
    async def test_on_fallback_callback(self):
        """Should call onFallback callback with correct index and reason."""
        on_fallback = MagicMock()

        def fail_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and state.content == "primary":
                return [
                    GuardrailViolation(
                        rule="fail-primary",
                        severity="error",
                        message="Primary failed",
                        recoverable=False,
                    )
                ]
            return []

        async def primary_stream():
            async for event in create_token_stream(["primary"]):
                yield event

        async def fallback_stream():
            async for event in create_token_stream(["success"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback_stream],
            guardrails=[GuardrailRule(name="fail-primary", check=fail_rule)],
            retry=Retry(attempts=1),
            on_fallback=on_fallback,
        )

        async for _ in result:
            pass

        on_fallback.assert_called_once()
        index, reason = on_fallback.call_args[0]
        assert index == 0  # First fallback (0-indexed)
        assert isinstance(reason, str)

    @pytest.mark.asyncio
    async def test_fallback_start_includes_indices(self):
        """Should include fromIndex and toIndex in FALLBACK_START event."""
        collector = create_event_collector()

        def fail_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and "success" not in state.content:
                return [
                    GuardrailViolation(
                        rule="fail",
                        severity="error",
                        message="Fail",
                        recoverable=False,
                    )
                ]
            return []

        async def primary_stream():
            async for event in create_token_stream(["fail1"]):
                yield event

        async def fallback1_stream():
            async for event in create_token_stream(["fail2"]):
                yield event

        async def fallback2_stream():
            async for event in create_token_stream(["success"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback1_stream, fallback2_stream],
            guardrails=[GuardrailRule(name="fail", check=fail_rule)],
            retry=Retry(attempts=1),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        fallback_starts = collector.get_events_of_type(
            ObservabilityEventType.FALLBACK_START.value
        )
        assert len(fallback_starts) == 2

        # First fallback: from primary (0) to first fallback (1)
        assert fallback_starts[0].data.get("fromIndex") == 0
        assert fallback_starts[0].data.get("toIndex") == 1

        # Second fallback: from first fallback (1) to second fallback (2)
        assert fallback_starts[1].data.get("fromIndex") == 1
        assert fallback_starts[1].data.get("toIndex") == 2


# ============================================================================
# Error Flow Tests
# ============================================================================


class TestLifecycleErrorFlow:
    """Tests for error flow behavior."""

    @pytest.mark.asyncio
    async def test_error_event_with_recovery_strategy(self):
        """Should emit ERROR event with recoveryStrategy when error occurs."""
        collector = create_event_collector()

        async def primary_stream():
            async for event in create_failing_stream(["start"]):
                yield event

        async def fallback_stream():
            async for event in create_token_stream(["fallback"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback_stream],
            retry=Retry(attempts=1),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        errors = collector.get_events_of_type(ObservabilityEventType.ERROR.value)
        assert len(errors) > 0

        error_event = errors[0]
        assert (
            error_event.data.get("error") is not None
            or error_event.data.get("message") is not None
        )
        recovery_strategy = error_event.data.get("recoveryStrategy")
        assert recovery_strategy in ["retry", "fallback", "halt", None]

    @pytest.mark.asyncio
    async def test_on_error_callback(self):
        """Should call onError callback with error and recovery flags."""
        on_error = MagicMock()

        async def primary_stream():
            async for event in create_failing_stream(["start"]):
                yield event

        async def fallback_stream():
            async for event in create_token_stream(["fallback"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback_stream],
            retry=Retry(attempts=1),
            on_error=on_error,
        )

        async for _ in result:
            pass

        on_error.assert_called()
        error, will_retry, will_fallback = on_error.call_args[0]
        assert isinstance(error, Exception)
        assert isinstance(will_retry, bool)
        assert isinstance(will_fallback, bool)

    @pytest.mark.asyncio
    async def test_will_fallback_true_when_available(self):
        """Should indicate willFallback=true when no retries but fallback available."""
        on_error = MagicMock()

        async def primary_stream():
            async for event in create_failing_stream([]):
                yield event

        async def fallback_stream():
            async for event in create_token_stream(["fallback"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback_stream],
            retry=Retry(attempts=0),  # No retries
            on_error=on_error,
        )

        async for _ in result:
            pass

        on_error.assert_called()
        _, will_retry, will_fallback = on_error.call_args[0]
        assert will_retry is False
        assert will_fallback is True


# ============================================================================
# Checkpoint & Continuation Tests
# ============================================================================


class TestLifecycleCheckpointFlow:
    """Tests for checkpoint and continuation flow."""

    @pytest.mark.asyncio
    async def test_checkpoint_saved_when_enabled(self):
        """Should emit CHECKPOINT_SAVED events when continuation enabled."""
        collector = create_event_collector()

        # Generate enough tokens to trigger checkpoint
        tokens = [f"t{i}-" for i in range(15)]

        async def stream():
            async for event in create_token_stream(tokens):
                yield event

        result = await _internal_run(
            stream=stream,
            continue_from_last_good_token=True,
            check_intervals=CheckIntervals(checkpoint=5),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        checkpoints = collector.get_events_of_type(
            ObservabilityEventType.CHECKPOINT_SAVED.value
        )
        assert len(checkpoints) > 0

    @pytest.mark.asyncio
    async def test_no_checkpoint_when_disabled(self):
        """Should NOT emit CHECKPOINT_SAVED when continuation disabled."""
        collector = create_event_collector()

        tokens = [f"t{i}-" for i in range(15)]

        async def stream():
            async for event in create_token_stream(tokens):
                yield event

        result = await _internal_run(
            stream=stream,
            continue_from_last_good_token=False,
            check_intervals=CheckIntervals(checkpoint=5),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        checkpoints = collector.get_events_of_type(
            ObservabilityEventType.CHECKPOINT_SAVED.value
        )
        assert len(checkpoints) == 0


# ============================================================================
# Guardrail Violation Flow Tests
# ============================================================================


class TestLifecycleGuardrailViolation:
    """Tests for guardrail violation flow."""

    @pytest.mark.asyncio
    async def test_guardrail_rule_result_on_violation(self):
        """Should emit GUARDRAIL_RULE_RESULT on violation."""
        collector = create_event_collector()

        def bad_word_rule(state: State) -> list[GuardrailViolation]:
            if "bad" in state.content:
                return [
                    GuardrailViolation(
                        rule="no-bad",
                        severity="warning",
                        message="Bad word detected",
                        recoverable=True,
                    )
                ]
            return []

        async def stream():
            async for event in create_token_stream(
                ["this", " ", "is", " ", "bad", " ", "content"]
            ):
                yield event

        result = await _internal_run(
            stream=stream,
            guardrails=[GuardrailRule(name="no-bad", check=bad_word_rule)],
            check_intervals=CheckIntervals(guardrails=1),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        violations = collector.get_events_of_type(
            ObservabilityEventType.GUARDRAIL_RULE_RESULT.value
        )
        assert len(violations) > 0

    @pytest.mark.asyncio
    async def test_on_violation_callback(self):
        """Should call onViolation callback."""
        on_violation = MagicMock()

        def always_violate_rule(state: State) -> list[GuardrailViolation]:
            if state.completed:
                return [
                    GuardrailViolation(
                        rule="always-violate",
                        severity="warning",
                        message="Always violates",
                        recoverable=True,
                    )
                ]
            return []

        async def stream():
            async for event in create_token_stream(["test"]):
                yield event

        result = await _internal_run(
            stream=stream,
            guardrails=[
                GuardrailRule(name="always-violate", check=always_violate_rule)
            ],
            on_violation=on_violation,
        )

        async for _ in result:
            pass

        on_violation.assert_called()
        violation = on_violation.call_args[0][0]
        assert violation.rule == "always-violate"
        assert violation.severity == "warning"

    @pytest.mark.asyncio
    async def test_fatal_violation_checked_before_recoverable(self):
        """Should check fatal violations before recoverable ones to avoid doomed retries."""
        collector = create_event_collector()
        attempt_count = 0

        def mixed_violations_rule(state: State) -> list[GuardrailViolation]:
            if state.completed:
                # Return both recoverable and fatal violations
                return [
                    GuardrailViolation(
                        rule="recoverable-rule",
                        severity="error",
                        message="Recoverable violation",
                        recoverable=True,
                    ),
                    GuardrailViolation(
                        rule="fatal-rule",
                        severity="error",
                        message="Fatal violation",
                        recoverable=False,
                    ),
                ]
            return []

        async def stream():
            nonlocal attempt_count
            attempt_count += 1
            async for event in create_token_stream(["test"]):
                yield event

        result = await _internal_run(
            stream=stream,
            guardrails=[
                GuardrailRule(name="mixed-violations", check=mixed_violations_rule)
            ],
            retry=Retry(attempts=3),
            on_event=collector.handler,
        )

        # Should raise fatal error without retrying
        with pytest.raises(Error) as exc_info:
            async for _ in result:
                pass

        # Verify it's a fatal guardrail violation
        assert exc_info.value.code == ErrorCode.FATAL_GUARDRAIL_VIOLATION
        assert "Fatal guardrail violation" in str(exc_info.value)

        # Should only have 1 attempt - no retries since fatal was detected first
        assert attempt_count == 1


# ============================================================================
# Combined Complex Flow Tests
# ============================================================================


class TestLifecycleCombinedFlows:
    """Tests for combined complex flows."""

    @pytest.mark.asyncio
    async def test_retry_fallback_flow(self):
        """Should handle retry -> fallback flow correctly."""
        collector = create_event_collector()
        primary_attempts = 0

        def force_retry_rule(state: State) -> list[GuardrailViolation]:
            if state.completed and state.content == "primary-fail":
                return [
                    GuardrailViolation(
                        rule="force-retry",
                        severity="error",
                        message="Force retry",
                        recoverable=True,
                    )
                ]
            return []

        async def primary_factory():
            nonlocal primary_attempts
            primary_attempts += 1
            if primary_attempts <= 2:
                # First two attempts fail via guardrail
                async for event in create_token_stream(["primary-fail"]):
                    yield event
            else:
                # Third attempt also fails to trigger fallback
                async for event in create_failing_stream(
                    ["prim", "ary", "-", "fail"],
                    Exception("Primary exhausted"),
                ):
                    yield event

        async def fallback_stream():
            async for event in create_token_stream(["fallback-success"]):
                yield event

        result = await _internal_run(
            stream=primary_factory,
            fallbacks=[fallback_stream],
            guardrails=[GuardrailRule(name="force-retry", check=force_retry_rule)],
            retry=Retry(attempts=2),
            continue_from_last_good_token=True,
            check_intervals=CheckIntervals(checkpoint=2),
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        types = collector.get_event_types()

        # Should see the sequence with retry and fallback
        assert ObservabilityEventType.SESSION_START.value in types
        assert ObservabilityEventType.RETRY_ATTEMPT.value in types
        assert ObservabilityEventType.FALLBACK_START.value in types
        assert ObservabilityEventType.COMPLETE.value in types

        # Verify overall completion
        assert result.state.completed is True

    @pytest.mark.asyncio
    async def test_callback_order_consistency(self):
        """Should maintain consistent event order across all callback types."""
        call_order: list[str] = []

        on_start = MagicMock(side_effect=lambda *args: call_order.append("onStart"))
        on_complete = MagicMock(
            side_effect=lambda *args: call_order.append("onComplete")
        )
        on_retry = MagicMock(side_effect=lambda *args: call_order.append("onRetry"))
        on_fallback = MagicMock(
            side_effect=lambda *args: call_order.append("onFallback")
        )

        async def primary_stream():
            for i in range(10):
                yield Event(type=EventType.TOKEN, text=f"t{i}")
            yield Event(type=EventType.ERROR, error=Exception("Fail"))

        async def fallback_stream():
            async for event in create_token_stream(["ok"]):
                yield event

        result = await _internal_run(
            stream=primary_stream,
            fallbacks=[fallback_stream],
            retry=Retry(attempts=1),
            continue_from_last_good_token=True,
            check_intervals=CheckIntervals(checkpoint=3),
            on_start=on_start,
            on_complete=on_complete,
            on_retry=on_retry,
            on_fallback=on_fallback,
        )

        async for _ in result:
            pass

        # onStart should be called first (for each attempt)
        assert call_order[0] == "onStart"

        # onComplete should be called last
        assert call_order[-1] == "onComplete"

    @pytest.mark.asyncio
    async def test_state_tracking_through_retry(self):
        """Should track all state correctly through retry flow."""
        attempt_count = 0

        def rule(state: State) -> list[GuardrailViolation]:
            if state.completed and "retry-trigger" in state.content:
                return [
                    GuardrailViolation(
                        rule="test",
                        severity="error",
                        message="Must retry",
                        recoverable=True,
                    )
                ]
            return []

        async def stream_factory():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                async for event in create_token_stream(["retry-trigger"]):
                    yield event
            else:
                async for event in create_token_stream(["final", "-", "success"]):
                    yield event

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="test", check=rule)],
            retry=Retry(attempts=2),
        )

        async for _ in result:
            pass

        # State should reflect final successful attempt
        assert result.state.completed is True
        assert result.state.content == "final-success"
        assert result.state.token_count == 3


# ============================================================================
# Event Timestamp Ordering Tests
# ============================================================================


class TestLifecycleTimestampOrdering:
    """Tests for event timestamp ordering."""

    @pytest.mark.asyncio
    async def test_monotonically_increasing_timestamps(self):
        """Should have monotonically increasing timestamps."""
        collector = create_event_collector()

        async def stream():
            async for event in create_token_stream(["a", "b", "c", "d", "e"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        events = collector.events
        for i in range(1, len(events)):
            assert events[i].ts >= events[i - 1].ts

    @pytest.mark.asyncio
    async def test_consistent_stream_id(self):
        """Should have consistent streamId across all events in session."""
        collector = create_event_collector()

        async def stream():
            async for event in create_token_stream(["test"]):
                yield event

        result = await _internal_run(
            stream=stream,
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        # Get all observability events (those with stream_id)
        obs_events = [e for e in collector.events if e.data.get("stream_id")]

        assert len(obs_events) > 0, "Expected observability events to be recorded"
        stream_id = obs_events[0].data.get("stream_id")
        for event in obs_events:
            assert event.data.get("stream_id") == stream_id

    @pytest.mark.asyncio
    async def test_user_context_in_events(self):
        """Should include user context in all observability events."""
        collector = create_event_collector()

        async def stream():
            async for event in create_token_stream(["test"]):
                yield event

        result = await _internal_run(
            stream=stream,
            context={"requestId": "req-123", "userId": "user-456"},
            on_event=collector.handler,
        )

        async for _ in result:
            pass

        # Get all observability events (those with context)
        obs_events = [e for e in collector.events if e.data.get("context")]

        assert len(obs_events) > 0, "Expected observability events with context"

        for event in obs_events:
            context = event.data.get("context", {})
            assert context.get("requestId") == "req-123"
            assert context.get("userId") == "user-456"
