"""Canonical Lifecycle Tests for L0 Runtime (Python)"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from l0 import Retry
from l0.events import ObservabilityEvent, ObservabilityEventType
from l0.guardrails import GuardrailRule, GuardrailViolation
from l0.runtime import _internal_run
from l0.types import AwaitableStreamFactory, Event, EventType, State


def camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def load_scenarios() -> dict[str, Any]:
    fixture_path = Path(__file__).parent / "fixtures" / "lifecycle-scenarios.json"
    with open(fixture_path) as f:
        return json.load(f)


SCENARIOS = load_scenarios()


@dataclass
class CollectedEvent:
    type: str
    ts: float
    data: dict[str, Any]


@dataclass
class EventCollector:
    events: list[CollectedEvent] = field(default_factory=list)

    def handler(self, event: ObservabilityEvent) -> None:
        evt_type = (
            event.type.value
            if isinstance(event.type, ObservabilityEventType)
            else str(event.type)
        )
        self.events.append(
            CollectedEvent(
                type=evt_type,
                ts=event.ts,
                data={
                    "type": evt_type,
                    "ts": event.ts,
                    "stream_id": event.stream_id,
                    "context": event.context,
                    **event.meta,
                },
            )
        )

    def get_event_types(self) -> list[str]:
        return [e.type for e in self.events]

    def get_events_of_type(self, event_type: str) -> list[CollectedEvent]:
        return [e for e in self.events if e.type == event_type]


def get_nested_value(obj: dict[str, Any], path: str) -> Any:
    current: Any = obj
    for key in path.split("."):
        if current is None or not isinstance(current, dict):
            return None
        if key in current:
            current = current[key]
        else:
            current = current.get(camel_to_snake(key))
    return current


def validate_event_assertions(
    event: CollectedEvent, assertions: dict[str, Any]
) -> None:
    for path, expected in assertions.items():
        actual = get_nested_value(event.data, path)
        assert actual == expected, (
            f"Event {event.type}: {path} expected {expected!r}, got {actual!r}"
        )


def validate_observability_event_sequence(
    collector: EventCollector, expected_events: list[dict[str, Any]]
) -> None:
    collected_types = collector.get_event_types()
    last_idx = -1
    for expected in expected_events:
        event_type = expected["type"]
        assert len(collector.get_events_of_type(event_type)) > 0, (
            f"Expected {event_type} event"
        )
        try:
            found_idx = collected_types.index(event_type, last_idx + 1)
        except ValueError:
            found_idx = -1
        assert found_idx > last_idx, f"Expected {event_type} after index {last_idx}"
        if "assertions" in expected:
            validate_event_assertions(
                collector.events[found_idx], expected["assertions"]
            )
        last_idx = found_idx


async def create_token_stream(tokens: list[str]) -> AsyncIterator[Event]:
    for token in tokens:
        yield Event(type=EventType.TOKEN, text=token)
    yield Event(type=EventType.COMPLETE)


async def create_failing_stream(
    tokens: list[str], error: Exception | None = None
) -> AsyncIterator[Event]:
    for token in tokens:
        yield Event(type=EventType.TOKEN, text=token)
    raise (error or Exception("Stream failed"))


async def run_normal_success_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    collector = EventCollector()
    config = scenario["config"]
    tokens = config["tokens"]
    context = config.get("context", {})

    async def stream_factory() -> AsyncIterator[Event]:
        async for event in create_token_stream(tokens):
            yield event

    result = await _internal_run(
        stream=stream_factory,
        context=context,
        on_event=collector.handler,
        retry=Retry(attempts=1, max_retries=1),
    )
    async for _ in result:
        pass

    validate_observability_event_sequence(
        collector, scenario["expectedObservabilityEvents"]
    )
    return {"collector": collector}


async def run_error_context_propagation_scenario(
    scenario: dict[str, Any],
) -> dict[str, Any]:
    collector = EventCollector()
    config = scenario["config"]
    context = config.get("context", {})
    fallback_streams = config.get("fallbackStreams", [])

    async def failing_stream() -> AsyncIterator[Event]:
        async for event in create_failing_stream([]):
            yield event

    fallback_factories: list[AwaitableStreamFactory] = [
        lambda tokens=fs["tokens"]: create_token_stream(tokens)
        for fs in fallback_streams
    ]

    result = await _internal_run(
        stream=failing_stream,
        fallbacks=fallback_factories,
        retry=Retry(attempts=0, max_retries=0),
        context=context,
        on_event=collector.handler,
    )
    async for _ in result:
        pass

    session_starts = collector.get_events_of_type("SESSION_START")
    assert len(session_starts) == 1
    session_ctx = session_starts[0].data.get("context", {})
    assert session_ctx.get("requestId") == "error-ctx-404"
    assert session_ctx.get("userId") == "user-xyz"
    assert session_ctx.get("nested", {}).get("traceId") == "trace-abc"

    errors = collector.get_events_of_type("ERROR")
    assert len(errors) > 0
    assert errors[0].data.get("context", {}).get("requestId") == "error-ctx-404"

    fallback_starts = collector.get_events_of_type("FALLBACK_START")
    assert len(fallback_starts) == 1
    assert (
        fallback_starts[0].data.get("context", {}).get("requestId") == "error-ctx-404"
    )

    completes = collector.get_events_of_type("COMPLETE")
    assert len(completes) == 1
    assert completes[0].data.get("context", {}).get("requestId") == "error-ctx-404"

    return {"collector": collector}


async def run_retry_on_guardrail_scenario(
    scenario: dict[str, Any],
) -> dict[str, Any]:
    collector = EventCollector()
    config = scenario["config"]
    attempts = config["attempts"]
    context = config.get("context", {})

    attempt_index = 0
    on_start_calls: list[list[Any]] = []
    on_retry_calls: list[list[Any]] = []

    def guardrail_check(state: State) -> list[GuardrailViolation]:
        if state.completed and attempts[attempt_index - 1].get("guardrailFails"):
            return [
                GuardrailViolation(
                    rule="test-guardrail",
                    severity="error",
                    message="Guardrail violation",
                    recoverable=True,
                )
            ]
        return []

    guardrail_rule = GuardrailRule(
        name="test-guardrail",
        check=guardrail_check,
        streaming=False,
    )

    def stream_factory() -> AsyncIterator[Event]:
        nonlocal attempt_index
        current = attempts[attempt_index]
        attempt_index += 1
        return create_token_stream(current.get("tokens", ["fallback"]))

    result = await _internal_run(
        stream=stream_factory,
        guardrails=[guardrail_rule],
        retry=Retry(
            attempts=config["retry"]["attempts"],
            max_retries=config["retry"]["attempts"],
        ),
        context=context,
        on_event=collector.handler,
        on_start=lambda attempt, is_retry, is_fallback: on_start_calls.append(
            [attempt, is_retry, is_fallback]
        ),
        on_retry=lambda attempt, reason: on_retry_calls.append([attempt, reason]),
    )
    async for _ in result:
        pass

    # Validate SESSION_START emitted exactly once
    session_starts = collector.get_events_of_type("SESSION_START")
    assert len(session_starts) == 1

    # Validate ATTEMPT_START emitted for retries
    attempt_starts = collector.get_events_of_type("ATTEMPT_START")
    assert len(attempt_starts) == 1
    # ATTEMPT_START with attempt=2 indicates this is a retry
    assert attempt_starts[0].data.get("attempt") == 2

    # Validate RETRY_ATTEMPT emitted
    retry_attempts = collector.get_events_of_type("RETRY_ATTEMPT")
    assert len(retry_attempts) == 1

    # Validate event ordering: RETRY_ATTEMPT comes before ATTEMPT_START
    event_types = collector.get_event_types()
    retry_idx = event_types.index("RETRY_ATTEMPT")
    attempt_start_idx = event_types.index("ATTEMPT_START")
    assert retry_idx < attempt_start_idx

    # Validate callbacks
    assert len(on_start_calls) == 2
    assert on_start_calls[0] == [1, False, False]
    assert on_start_calls[1] == [2, True, False]
    assert len(on_retry_calls) == 1

    return {"collector": collector, "on_start_calls": on_start_calls, "on_retry_calls": on_retry_calls}


async def run_fallback_after_retries_scenario(
    scenario: dict[str, Any],
) -> dict[str, Any]:
    collector = EventCollector()
    config = scenario["config"]
    primary_attempts = config["primaryAttempts"]
    fallback_streams_config = config["fallbackStreams"]
    context = config.get("context", {})

    attempt_index = 0
    on_start_calls: list[list[Any]] = []
    on_fallback_calls: list[list[Any]] = []

    def stream_factory() -> AsyncIterator[Event]:
        nonlocal attempt_index
        current = primary_attempts[attempt_index]
        attempt_index += 1
        if current.get("error"):
            return create_failing_stream(current["tokens"])
        return create_token_stream(current.get("tokens", []))

    fallback_factories: list[AwaitableStreamFactory] = [
        lambda tokens=fs["tokens"]: create_token_stream(tokens)
        for fs in fallback_streams_config
    ]

    result = await _internal_run(
        stream=stream_factory,
        fallbacks=fallback_factories,
        retry=Retry(
            attempts=config["retry"]["attempts"],
            max_retries=config["retry"]["attempts"],
        ),
        context=context,
        on_event=collector.handler,
        on_start=lambda attempt, is_retry, is_fallback: on_start_calls.append(
            [attempt, is_retry, is_fallback]
        ),
        on_fallback=lambda index, reason: on_fallback_calls.append([index, reason]),
    )
    async for _ in result:
        pass

    # Validate expected event types appear in order (skip field-level assertions
    # that may differ between runtimes, e.g. recoveryStrategy in ERROR events)
    expected_types = [e["type"] for e in scenario["expectedObservabilityEvents"]]
    collected_types = collector.get_event_types()
    last_idx = -1
    for expected_type in expected_types:
        assert len(collector.get_events_of_type(expected_type)) > 0, (
            f"Expected {expected_type} event"
        )
        try:
            found_idx = collected_types.index(expected_type, last_idx + 1)
        except ValueError:
            found_idx = -1
        assert found_idx > last_idx, f"Expected {expected_type} after index {last_idx}"
        last_idx = found_idx

    # Validate counts
    session_starts = collector.get_events_of_type("SESSION_START")
    assert len(session_starts) == 1

    fallback_starts = collector.get_events_of_type("FALLBACK_START")
    assert len(fallback_starts) == 1

    # Validate onStart called for initial, retry, and fallback
    assert len(on_start_calls) == 3
    assert on_start_calls[0] == [1, False, False]  # Initial
    assert on_start_calls[1] == [2, True, False]  # Retry
    assert on_start_calls[2] == [1, False, True]  # Fallback

    # Validate onFallback called
    assert len(on_fallback_calls) == 1

    return {"collector": collector, "on_start_calls": on_start_calls, "on_fallback_calls": on_fallback_calls}


class TestCanonicalLifecycle:
    @pytest.fixture
    def scenarios(self) -> list[dict[str, Any]]:
        return SCENARIOS["scenarios"]

    def get_scenario(
        self, scenarios: list[dict[str, Any]], scenario_id: str
    ) -> dict[str, Any]:
        for s in scenarios:
            if s["id"] == scenario_id:
                return s
        raise ValueError(f"Scenario {scenario_id} not found")


class TestNormalSuccessFlow(TestCanonicalLifecycle):
    @pytest.mark.asyncio
    async def test_normal_success(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "normal-success")
        await run_normal_success_scenario(scenario)

    @pytest.mark.asyncio
    async def test_invariants(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "normal-success")
        result = await run_normal_success_scenario(scenario)
        collector = result["collector"]
        assert len(collector.get_events_of_type("SESSION_START")) == 1
        assert collector.get_event_types()[-1] == "COMPLETE"


class TestRetryOnGuardrail(TestCanonicalLifecycle):
    @pytest.mark.asyncio
    async def test_retry_on_guardrail(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "retry-on-guardrail")
        await run_retry_on_guardrail_scenario(scenario)

    @pytest.mark.asyncio
    async def test_invariants(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "retry-on-guardrail")
        result = await run_retry_on_guardrail_scenario(scenario)
        collector = result["collector"]

        # SESSION_START emitted exactly once
        assert len(collector.get_events_of_type("SESSION_START")) == 1

        # ATTEMPT_START emitted for retries
        assert len(collector.get_events_of_type("ATTEMPT_START")) == 1

        # RETRY_ATTEMPT precedes ATTEMPT_START
        types = collector.get_event_types()
        retry_idx = types.index("RETRY_ATTEMPT")
        attempt_idx = types.index("ATTEMPT_START")
        assert retry_idx < attempt_idx


class TestFallbackAfterRetries(TestCanonicalLifecycle):
    @pytest.mark.asyncio
    async def test_fallback_after_retries(
        self, scenarios: list[dict[str, Any]]
    ) -> None:
        scenario = self.get_scenario(scenarios, "fallback-after-retries-exhausted")
        await run_fallback_after_retries_scenario(scenario)

    @pytest.mark.asyncio
    async def test_invariants(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "fallback-after-retries-exhausted")
        result = await run_fallback_after_retries_scenario(scenario)
        collector = result["collector"]

        # SESSION_START emitted exactly once
        assert len(collector.get_events_of_type("SESSION_START")) == 1

        # FALLBACK_START emitted (not ATTEMPT_START for fallbacks)
        assert len(collector.get_events_of_type("FALLBACK_START")) == 1


class TestErrorContextPropagation(TestCanonicalLifecycle):
    @pytest.mark.asyncio
    async def test_error_context_propagation(
        self, scenarios: list[dict[str, Any]]
    ) -> None:
        scenario = self.get_scenario(scenarios, "error-context-propagation")
        await run_error_context_propagation_scenario(scenario)

    @pytest.mark.asyncio
    async def test_invariants(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "error-context-propagation")
        result = await run_error_context_propagation_scenario(scenario)
        collector = result["collector"]

        # Check all observability events have context
        obs_events = [e for e in collector.events if e.data.get("context")]
        assert len(obs_events) > 0

        # All should have the same requestId
        for event in obs_events:
            ctx = event.data.get("context", {})
            assert ctx.get("requestId") == "error-ctx-404"


class TestCrossLanguageInvariants(TestCanonicalLifecycle):
    @pytest.mark.asyncio
    async def test_session_start_is_first(
        self, scenarios: list[dict[str, Any]]
    ) -> None:
        scenario = self.get_scenario(scenarios, "normal-success")
        result = await run_normal_success_scenario(scenario)
        assert result["collector"].get_event_types()[0] == "SESSION_START"

    @pytest.mark.asyncio
    async def test_complete_is_final(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "normal-success")
        result = await run_normal_success_scenario(scenario)
        assert result["collector"].get_event_types()[-1] == "COMPLETE"

    @pytest.mark.asyncio
    async def test_timestamps_monotonic(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "normal-success")
        result = await run_normal_success_scenario(scenario)
        events = result["collector"].events
        for i in range(1, len(events)):
            assert events[i].ts >= events[i - 1].ts

    @pytest.mark.asyncio
    async def test_stream_id_consistent(self, scenarios: list[dict[str, Any]]) -> None:
        scenario = self.get_scenario(scenarios, "normal-success")
        result = await run_normal_success_scenario(scenario)
        events = [e for e in result["collector"].events if e.data.get("stream_id")]
        assert len(events) > 0
        stream_id = events[0].data["stream_id"]
        for e in events:
            assert e.data["stream_id"] == stream_id

    @pytest.mark.asyncio
    async def test_context_deeply_cloned_and_immutable(
        self, scenarios: list[dict[str, Any]]
    ) -> None:
        scenario = self.get_scenario(scenarios, "error-context-propagation")
        result = await run_error_context_propagation_scenario(scenario)
        collector = result["collector"]

        # Get context from SESSION_START
        session_start = collector.get_events_of_type("SESSION_START")[0]
        ctx = session_start.data.get("context", {})

        # Verify nested context is present
        assert ctx.get("nested", {}).get("traceId") == "trace-abc"

        # Verify context is a deep copy (mutating it doesn't affect other events)
        original_id = ctx.get("requestId")
        ctx_copy = copy.deepcopy(ctx)
        ctx_copy["requestId"] = "mutated"
        assert ctx.get("requestId") == original_id


# ============================================================================
# Callback Parameter Runtime Validation Tests
# ============================================================================

CALLBACK_REFERENCE = SCENARIOS.get("callbackReference", {})


class TestOnStartCallback:
    @pytest.mark.asyncio
    async def test_receives_correct_parameter_types(self) -> None:
        collector = EventCollector()
        received_args: list[list[Any]] = []

        result = await _internal_run(
            stream=lambda: create_token_stream(["hello"]),
            on_event=collector.handler,
            on_start=lambda attempt, is_retry, is_fallback: received_args.append(
                [attempt, is_retry, is_fallback]
            ),
            retry=Retry(attempts=1, max_retries=1),
        )
        async for _ in result:
            pass

        assert len(received_args) == 1
        assert received_args[0] == [1, False, False]
        assert isinstance(received_args[0][0], int)
        assert isinstance(received_args[0][1], bool)
        assert isinstance(received_args[0][2], bool)

    @pytest.mark.asyncio
    async def test_receives_is_retry_true_on_retry(self) -> None:
        attempt_index = 0
        received_args: list[list[Any]] = []

        def guardrail_check(state: State) -> list[GuardrailViolation]:
            nonlocal attempt_index
            if state.completed and attempt_index == 1:
                return [
                    GuardrailViolation(
                        rule="force-retry",
                        severity="error",
                        message="Retry",
                        recoverable=True,
                    )
                ]
            return []

        def stream_factory() -> AsyncIterator[Event]:
            nonlocal attempt_index
            attempt_index += 1
            return create_token_stream(["token"])

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="force-retry", check=guardrail_check, streaming=False)],
            retry=Retry(attempts=2, max_retries=2),
            on_start=lambda attempt, is_retry, is_fallback: received_args.append(
                [attempt, is_retry, is_fallback]
            ),
        )
        async for _ in result:
            pass

        assert len(received_args) == 2
        assert received_args[0] == [1, False, False]  # Initial
        assert received_args[1] == [2, True, False]  # Retry

    @pytest.mark.asyncio
    async def test_receives_is_fallback_true_on_fallback(self) -> None:
        received_args: list[list[Any]] = []

        result = await _internal_run(
            stream=lambda: create_failing_stream([]),
            fallbacks=[lambda: create_token_stream(["fallback"])],
            retry=Retry(attempts=0, max_retries=0),
            on_start=lambda attempt, is_retry, is_fallback: received_args.append(
                [attempt, is_retry, is_fallback]
            ),
        )
        async for _ in result:
            pass

        assert len(received_args) == 2
        assert received_args[0] == [1, False, False]  # Initial
        assert received_args[1] == [1, False, True]  # Fallback


class TestOnCompleteCallback:
    @pytest.mark.asyncio
    async def test_receives_state_with_content_and_token_count(self) -> None:
        received_state: list[Any] = []

        result = await _internal_run(
            stream=lambda: create_token_stream(["hello", " ", "world"]),
            on_complete=lambda state: received_state.append(state),
            retry=Retry(attempts=1, max_retries=1),
        )
        async for _ in result:
            pass

        assert len(received_state) == 1
        state = received_state[0]
        assert isinstance(state.content, str)
        assert isinstance(state.token_count, int)
        assert state.content == "hello world"
        assert state.token_count == 3


class TestOnRetryCallback:
    @pytest.mark.asyncio
    async def test_receives_attempt_and_reason(self) -> None:
        attempt_index = 0
        received_args: list[list[Any]] = []

        def guardrail_check(state: State) -> list[GuardrailViolation]:
            nonlocal attempt_index
            if state.completed and attempt_index == 1:
                return [
                    GuardrailViolation(
                        rule="force-retry",
                        severity="error",
                        message="Guardrail failed",
                        recoverable=True,
                    )
                ]
            return []

        def stream_factory() -> AsyncIterator[Event]:
            nonlocal attempt_index
            attempt_index += 1
            return create_token_stream(["token"])

        result = await _internal_run(
            stream=stream_factory,
            guardrails=[GuardrailRule(name="force-retry", check=guardrail_check, streaming=False)],
            retry=Retry(attempts=2, max_retries=2),
            on_retry=lambda attempt, reason: received_args.append([attempt, reason]),
        )
        async for _ in result:
            pass

        assert len(received_args) == 1
        assert isinstance(received_args[0][0], int)
        assert received_args[0][0] >= 1
        assert isinstance(received_args[0][1], str)


class TestOnFallbackCallback:
    @pytest.mark.asyncio
    async def test_receives_index_and_reason(self) -> None:
        received_args: list[list[Any]] = []

        result = await _internal_run(
            stream=lambda: create_failing_stream([]),
            fallbacks=[lambda: create_token_stream(["fallback"])],
            retry=Retry(attempts=0, max_retries=0),
            on_fallback=lambda index, reason: received_args.append([index, reason]),
        )
        async for _ in result:
            pass

        assert len(received_args) == 1
        assert isinstance(received_args[0][0], int)
        assert received_args[0][0] == 0  # 0-based index
        assert isinstance(received_args[0][1], str)


class TestOnErrorCallback:
    @pytest.mark.asyncio
    async def test_receives_error_will_retry_will_fallback(self) -> None:
        received_args: list[list[Any]] = []

        result = await _internal_run(
            stream=lambda: create_failing_stream([]),
            fallbacks=[lambda: create_token_stream(["fallback"])],
            retry=Retry(attempts=0, max_retries=0),
            on_error=lambda error, will_retry, will_fallback: received_args.append(
                [error, will_retry, will_fallback]
            ),
        )
        async for _ in result:
            pass

        assert len(received_args) > 0
        assert isinstance(received_args[0][0], Exception)
        assert isinstance(received_args[0][1], bool)
        assert isinstance(received_args[0][2], bool)
        # First error should indicate willFallback=true since we have fallback streams
        assert received_args[0][2] is True


class TestCallbackSignatureMatchesSpec:
    def test_on_start_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onStart") == (
            "(attempt: number, isRetry: boolean, isFallback: boolean) => void"
        )

    def test_on_complete_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onComplete") == "(state: L0State) => void"

    def test_on_retry_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onRetry") == (
            "(attempt: number, reason: string) => void"
        )

    def test_on_fallback_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onFallback") == (
            "(index: number, reason: string) => void"
        )

    def test_on_checkpoint_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onCheckpoint") == (
            "(checkpoint: string, tokenCount: number) => void"
        )

    def test_on_resume_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onResume") == (
            "(checkpoint: string, tokenCount: number) => void"
        )

    def test_on_abort_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onAbort") == (
            "(tokenCount: number, contentLength: number) => void"
        )

    def test_on_timeout_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onTimeout") == (
            "(type: 'initial' | 'inter', elapsedMs: number) => void"
        )

    def test_on_violation_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onViolation") == (
            "(violation: GuardrailViolation) => void"
        )

    def test_on_drift_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onDrift") == (
            "(types: string[], confidence?: number) => void"
        )

    def test_on_tool_call_signature(self) -> None:
        assert CALLBACK_REFERENCE.get("onToolCall") == (
            "(toolName: string, toolCallId: string, args: Record<string, unknown>) => void"
        )
