"""Main L0 runtime engine."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Coroutine, cast

from .adapters import AdaptedEvent, Adapter, Adapters
from .continuation import ContinuationConfig, deduplicate_continuation, detect_overlap
from .drift import DriftDetector
from .errors import Error, ErrorCode, ErrorContext, FailureType, RecoveryStrategy
from .events import EventBus, ObservabilityEventType
from .logging import logger
from .retry import RetryManager
from .state import append_token, create_state, flush_content, mark_completed, update_checkpoint
from .types import (
    AwaitableStreamFactory,
    CheckIntervals,
    Event,
    EventType,
    RawStream,
    Retry,
    State,
    Stream,
    Timeout,
)


class TimeoutError(Exception):
    """Raised when a timeout occurs during streaming."""

    def __init__(self, message: str, timeout_type: str, timeout_seconds: float):
        super().__init__(message)
        self.timeout_type = timeout_type  # "initial_token" or "inter_token"
        self.timeout_seconds = timeout_seconds


# ─────────────────────────────────────────────────────────────────────────────
# Fast callback ID generation using UUIDv7 (time-ordered, faster than UUIDv4)
# ─────────────────────────────────────────────────────────────────────────────

import uuid6


def _next_callback_id() -> str:
    """Generate a callback ID using UUIDv7.

    UUIDv7 is time-ordered and faster than UUIDv4 since it uses
    timestamp + random bits instead of pure crypto-random.
    Provides global uniqueness for distributed tracing.
    """
    return f"cb_{uuid6.uuid7().hex[:12]}"


if TYPE_CHECKING:
    from .events import ObservabilityEvent
    from .guardrails import GuardrailRule, GuardrailViolation


def _get_failure_type(error: Exception) -> str:
    """Classify the failure type of an error (matches TS getFailureType)."""
    from .errors import Error as L0Error, ErrorCode as L0ErrorCode, is_network_error

    if isinstance(error, L0Error):
        code = error.code
        if code in (L0ErrorCode.INITIAL_TOKEN_TIMEOUT, L0ErrorCode.INTER_TOKEN_TIMEOUT):
            return FailureType.TIMEOUT.value
        if code == L0ErrorCode.ZERO_OUTPUT:
            return FailureType.ZERO_OUTPUT.value
        if code == L0ErrorCode.NETWORK_ERROR:
            return FailureType.NETWORK.value
        if code == L0ErrorCode.STREAM_ABORTED:
            return FailureType.ABORT.value

    if isinstance(error, TimeoutError):
        return FailureType.TIMEOUT.value

    if is_network_error(error):
        return FailureType.NETWORK.value

    return FailureType.UNKNOWN.value


def _get_recovery_strategy(will_retry: bool, will_fallback: bool) -> str:
    """Determine recovery strategy (matches TS getRecoveryStrategy)."""
    if will_retry:
        return RecoveryStrategy.RETRY.value
    if will_fallback:
        return RecoveryStrategy.FALLBACK.value
    return RecoveryStrategy.HALT.value


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle Callbacks Type
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class LifecycleCallbacks:
    """All lifecycle callbacks for L0 runtime.

    All callbacks are fire-and-forget - they never block the stream
    and errors in callbacks are silently caught.
    """

    on_start: Callable[[int, bool, bool], None] | None = None
    """Called when a new execution attempt begins.
    Args: (attempt: int, is_retry: bool, is_fallback: bool)
    """

    on_complete: Callable[[State], None] | None = None
    """Called when stream completes successfully.
    Args: (state: State)
    """

    on_error: Callable[[Exception, bool, bool], None] | None = None
    """Called when an error occurs (before retry/fallback decision).
    Args: (error: Exception, will_retry: bool, will_fallback: bool)
    """

    on_event: Callable[[Event], None] | None = None
    """Called for every L0 event emitted.
    Args: (event: Event)
    """

    on_violation: Callable[["GuardrailViolation"], None] | None = None
    """Called when a guardrail violation is detected.
    Args: (violation: GuardrailViolation)
    """

    on_retry: Callable[[int, str], None] | None = None
    """Called when a retry is triggered.
    Args: (attempt: int, reason: str)
    """

    on_fallback: Callable[[int, str], None] | None = None
    """Called when switching to a fallback model.
    Args: (index: int, reason: str)
    """

    on_resume: Callable[[str, int], None] | None = None
    """Called when resuming from a checkpoint.
    Args: (checkpoint: str, token_count: int)
    """

    on_checkpoint: Callable[[str, int], None] | None = None
    """Called when a checkpoint is saved.
    Args: (checkpoint: str, token_count: int)
    """

    on_timeout: Callable[[str, float], None] | None = None
    """Called when a timeout occurs.
    Args: (timeout_type: str, elapsed_seconds: float)
    """

    on_abort: Callable[[int, int], None] | None = None
    """Called when the stream is aborted.
    Args: (token_count: int, content_length: int)
    """

    on_drift: Callable[[list[str], float | None], None] | None = None
    """Called when drift is detected.
    Args: (drift_types: list[str], confidence: float | None)
    """

    on_tool_call: Callable[[str, str, dict[str, Any]], None] | None = None
    """Called when a tool call is detected.
    Args: (tool_name: str, tool_call_id: str, args: dict)
    """

    on_token: Callable[[str], None] | None = None
    """Called for each token received.
    Args: (text: str)
    """


def _fire_callback(callback: Callable[..., Any] | None, *args: Any) -> None:
    """Fire a callback without blocking or raising errors."""
    if callback is None:
        return
    try:
        callback(*args)
    except Exception as e:
        logger.debug(f"Callback error (silently caught): {e}")


def _validate_checkpoint(
    checkpoint: str,
    guardrails: list[GuardrailRule],
    event_bus: EventBus,
) -> bool:
    """Validate checkpoint content against guardrails.

    Returns True if checkpoint is valid for continuation, False otherwise.
    """
    if not checkpoint or not guardrails:
        return True

    # Create temporary state with checkpoint content
    temp_state = State(content=checkpoint, completed=False)

    has_fatal_violation = False

    for idx, rule in enumerate(guardrails):
        # Only check streaming rules (completion rules need completed=True)
        if not rule.streaming:
            continue

        violations = rule.check(temp_state)
        for v in violations:
            if v.severity == "error":
                has_fatal_violation = True
                event_bus.emit(
                    ObservabilityEventType.GUARDRAIL_RULE_RESULT,
                    index=idx,
                    ruleId=rule.name,
                    passed=False,
                    violation=v.__dict__,
                )

    return not has_fatal_violation


async def _internal_run(
    stream: AwaitableStreamFactory,
    *,
    fallbacks: list[AwaitableStreamFactory] | None = None,
    guardrails: list[GuardrailRule] | None = None,
    drift_detector: DriftDetector | None = None,
    retry: Retry | None = None,
    timeout: Timeout | None = None,
    check_intervals: CheckIntervals | None = None,
    adapter: Adapter | str | None = None,
    on_event: Callable[[ObservabilityEvent], None] | None = None,
    context: dict[str, Any] | None = None,
    buffer_tool_calls: bool = False,
    continue_from_last_good_token: ContinuationConfig | bool = False,
    build_continuation_prompt: Callable[[str], str] | None = None,
    # Lifecycle callbacks
    callbacks: LifecycleCallbacks | None = None,
    on_start: Callable[[int, bool, bool], None] | None = None,
    on_complete: Callable[[State], None] | None = None,
    on_error: Callable[[Exception, bool, bool], None] | None = None,
    on_stream_event: Callable[[Event], None] | None = None,
    on_violation: Callable[[GuardrailViolation], None] | None = None,
    on_retry: Callable[[int, str], None] | None = None,
    on_fallback: Callable[[int, str], None] | None = None,
    on_resume: Callable[[str, int], None] | None = None,
    on_checkpoint: Callable[[str, int], None] | None = None,
    on_timeout: Callable[[str, float], None] | None = None,
    on_abort: Callable[[int, int], None] | None = None,
    on_drift: Callable[[list[str], float | None], None] | None = None,
    on_tool_call: Callable[[str, str, dict[str, Any]], None] | None = None,
    on_token: Callable[[str], None] | None = None,
) -> "Stream[Any]":
    """Internal implementation of the L0 runtime.

    Args:
        stream: Factory function that returns an async LLM stream
        fallbacks: Optional list of fallback stream factories
        guardrails: Optional list of guardrail rules to apply
        drift_detector: Optional drift detector for detecting model derailment
        retry: Optional retry configuration
        timeout: Optional timeout configuration
        check_intervals: Optional check intervals for guardrails/drift/checkpoint
        adapter: Optional adapter hint ("openai", "litellm", or Adapter instance)
        on_event: Optional callback for observability events
        context: Optional user context attached to all events (request_id, tenant, etc.)
        buffer_tool_calls: Buffer tool call arguments until complete (default: False)
        continue_from_last_good_token: Resume from checkpoint on retry (default: False)
        build_continuation_prompt: Callback to modify prompt for continuation
        callbacks: Optional LifecycleCallbacks object with all callbacks
        on_start: Called when execution attempt begins
        on_complete: Called when stream completes
        on_error: Called when error occurs
        on_stream_event: Called for every L0 event
        on_violation: Called when guardrail violation detected
        on_retry: Called when retry triggered
        on_fallback: Called when switching to fallback
        on_resume: Called when resuming from checkpoint
        on_checkpoint: Called when checkpoint saved
        on_timeout: Called when timeout occurs
        on_abort: Called when stream aborted
        on_drift: Called when drift detected
        on_tool_call: Called when tool call detected
        on_token: Called for each token received (text)

    Returns:
        Stream - async iterator with .state, .abort(), and .read()
    """
    fallbacks = fallbacks or []
    guardrails = guardrails or []
    intervals = check_intervals or CheckIntervals()

    # Merge callbacks from LifecycleCallbacks object and individual params
    # Individual params take precedence
    cb = LifecycleCallbacks(
        on_start=on_start or (callbacks.on_start if callbacks else None),
        on_complete=on_complete or (callbacks.on_complete if callbacks else None),
        on_error=on_error or (callbacks.on_error if callbacks else None),
        on_event=on_stream_event or (callbacks.on_event if callbacks else None),
        on_violation=on_violation or (callbacks.on_violation if callbacks else None),
        on_retry=on_retry or (callbacks.on_retry if callbacks else None),
        on_fallback=on_fallback or (callbacks.on_fallback if callbacks else None),
        on_resume=on_resume or (callbacks.on_resume if callbacks else None),
        on_checkpoint=on_checkpoint or (callbacks.on_checkpoint if callbacks else None),
        on_timeout=on_timeout or (callbacks.on_timeout if callbacks else None),
        on_abort=on_abort or (callbacks.on_abort if callbacks else None),
        on_drift=on_drift or (callbacks.on_drift if callbacks else None),
        on_tool_call=on_tool_call or (callbacks.on_tool_call if callbacks else None),
        on_token=on_token or (callbacks.on_token if callbacks else None),
    )

    # Normalize continuation config
    continuation_config: ContinuationConfig | None = None
    if continue_from_last_good_token is True:
        continuation_config = ContinuationConfig.default()
    elif continue_from_last_good_token is False:
        continuation_config = None
    elif isinstance(continue_from_last_good_token, ContinuationConfig):
        continuation_config = (
            continue_from_last_good_token
            if continue_from_last_good_token.enabled
            else None
        )

    state = create_state()
    retry_mgr = RetryManager(retry)
    event_bus = EventBus(on_event, context=context)
    errors: list[Exception] = []
    aborted = False
    raw_chunks: list[Any] = []  # Collect raw chunks from provider
    attempt_number = 0  # Track attempt number for callbacks (1-based)

    # Track if we should use continuation on next retry
    use_continuation_on_retry = False
    pending_checkpoint: str | None = None

    logger.debug(f"Starting L0 stream: {event_bus.stream_id}")
    event_bus.emit(
        ObservabilityEventType.SESSION_START,
        attempt=1,
        isRetry=False,
        isFallback=False,
    )

    def abort() -> None:
        nonlocal aborted
        aborted = True
        state.aborted = True
        logger.debug("Abort requested")
        event_bus.emit(ObservabilityEventType.ABORT_REQUESTED, source="user")
        _fire_callback(cb.on_abort, state.token_count, len(state.content))

    async def run_stream() -> AsyncIterator[Event]:
        nonlocal \
            state, \
            use_continuation_on_retry, \
            pending_checkpoint, \
            raw_chunks, \
            attempt_number

        streams: list[AwaitableStreamFactory] = [stream] + fallbacks

        # Use check intervals from config, with continuation override for checkpoint
        checkpoint_interval = intervals.checkpoint
        guardrail_interval = intervals.guardrails
        drift_interval = intervals.drift

        # Continuation config can override checkpoint interval
        if continuation_config and continuation_config.checkpoint_interval:
            checkpoint_interval = continuation_config.checkpoint_interval

        for fallback_idx, stream_fn in enumerate(streams):
            state.fallback_index = fallback_idx
            is_fallback = fallback_idx > 0

            if is_fallback:
                logger.debug(f"Trying fallback {fallback_idx}")
                reason = "previous_failed"
                # Reset attempt number for fallback stream (TS parity)
                attempt_number = 0
                event_bus.emit(
                    ObservabilityEventType.FALLBACK_START,
                    fromIndex=fallback_idx - 1,
                    toIndex=fallback_idx,
                    reason=reason,
                )
                event_bus.emit(
                    ObservabilityEventType.FALLBACK_MODEL_SELECTED, index=fallback_idx
                )
                # on_fallback uses 0-based fallback index (0 = first fallback)
                # fallback_idx is 1-based in streams array (0 = primary, 1 = first fallback)
                _fire_callback(cb.on_fallback, fallback_idx - 1, reason)

            while True:
                if aborted:
                    event_bus.emit(
                        ObservabilityEventType.ABORT_COMPLETED,
                        tokenCount=state.token_count,
                        contentLength=len(state.content),
                    )
                    return

                # Increment attempt number (1-based for callbacks)
                attempt_number += 1
                is_retry = attempt_number > 1 and not is_fallback

                # Emit ATTEMPT_START for retry attempts (not initial or fallback)
                if is_retry:
                    event_bus.emit(
                        ObservabilityEventType.ATTEMPT_START,
                        attempt=attempt_number,
                        isRetry=True,
                        isFallback=is_fallback,
                    )

                # Fire on_start callback
                _fire_callback(cb.on_start, attempt_number, is_retry, is_fallback)

                # Check if we should emit continuation from checkpoint
                should_continue_from_checkpoint = (
                    use_continuation_on_retry
                    and pending_checkpoint
                    and continuation_config is not None
                )

                if (
                    should_continue_from_checkpoint
                    and pending_checkpoint
                    and continuation_config
                ):
                    # Validate checkpoint before using
                    checkpoint_valid = True
                    if continuation_config.validate_checkpoint and guardrails:
                        checkpoint_valid = _validate_checkpoint(
                            pending_checkpoint, guardrails, event_bus
                        )

                    if checkpoint_valid:
                        event_bus.emit(
                            ObservabilityEventType.CONTINUATION_START,
                            checkpointLength=len(pending_checkpoint),
                        )

                        # Emit RESUME_START per lifecycle spec
                        event_bus.emit(
                            ObservabilityEventType.RESUME_START,
                            checkpoint=pending_checkpoint,
                            tokenCount=state.token_count,
                        )

                        # Fire on_resume callback
                        _fire_callback(
                            cb.on_resume, pending_checkpoint, state.token_count
                        )

                        # Emit checkpoint content as tokens first
                        state.resume_point = pending_checkpoint
                        state.resume_from = 0
                        state.continuation_used = True

                        # Yield checkpoint as a single token event
                        # (The content is already in state from before the failure)
                        yield Event(
                            type=EventType.TOKEN, text=""
                        )  # Empty - content already tracked

                        # Call build_continuation_prompt if provided
                        if build_continuation_prompt:
                            # This allows the user to modify the prompt for the next call
                            build_continuation_prompt(pending_checkpoint)

                        logger.debug(
                            f"Continuing from checkpoint ({len(pending_checkpoint)} chars)"
                        )
                    else:
                        # Checkpoint invalid - start fresh
                        logger.debug("Checkpoint validation failed, starting fresh")
                        state.content = ""
                        state._content_buffer.clear()
                        state.token_count = 0
                        pending_checkpoint = None

                use_continuation_on_retry = False

                try:
                    event_bus.emit(ObservabilityEventType.STREAM_INIT)
                    raw_stream_or_coro: RawStream | Coroutine[Any, Any, RawStream] = (
                        stream_fn()
                    )

                    # Handle both sync and async stream factories
                    # stream_fn() might return a coroutine or an async iterator directly
                    raw_stream: RawStream
                    if inspect.iscoroutine(raw_stream_or_coro):
                        raw_stream = await raw_stream_or_coro
                    else:
                        raw_stream = cast(RawStream, raw_stream_or_coro)

                    event_bus.emit(ObservabilityEventType.ADAPTER_WRAP_START)
                    detected_adapter = Adapters.detect(raw_stream, adapter)
                    event_bus.emit(
                        ObservabilityEventType.ADAPTER_DETECTED,
                        adapterId=detected_adapter.name,
                    )
                    event_bus.emit(ObservabilityEventType.STREAM_READY)

                    # Set up timeout tracking
                    first_token_received = False
                    initial_timeout = (
                        timeout.initial_token / 1000 if timeout else None
                    )  # Convert ms to seconds
                    inter_timeout = (
                        timeout.inter_token / 1000 if timeout else None
                    )  # Convert ms to seconds

                    if initial_timeout is not None:
                        event_bus.emit(
                            ObservabilityEventType.TIMEOUT_START,
                            timeoutType="initial",
                            configuredMs=int(initial_timeout * 1000),
                        )

                    adapted_stream = detected_adapter.wrap(raw_stream)
                    event_bus.emit(ObservabilityEventType.ADAPTER_WRAP_END)

                    # Tool call buffering state (when buffer_tool_calls=True)
                    # Maps tool call index -> {id, name, arguments}
                    tool_call_buffers: dict[int, dict[str, Any]] = {}

                    # Track tool calls we've emitted TOOL_REQUESTED for (non-buffered mode)
                    tool_calls_started: set[str] = set()

                    async def emit_buffered_tool_calls() -> AsyncIterator[Event]:
                        """Emit all buffered tool calls."""
                        for idx in sorted(tool_call_buffers.keys()):
                            buffered = tool_call_buffers[idx]
                            tool_name = buffered.get("name", "")
                            tool_call_id = buffered.get("id", "")
                            tool_args = buffered.get("arguments", "")

                            # Parse arguments to dict for event emission
                            import json

                            parsed_args: dict[str, Any] = {}
                            if tool_args:
                                try:
                                    parsed_args = json.loads(tool_args)
                                except (json.JSONDecodeError, TypeError):
                                    parsed_args = {"_raw": tool_args}

                            # Emit TOOL_REQUESTED and TOOL_START
                            event_bus.emit(
                                ObservabilityEventType.TOOL_REQUESTED,
                                toolName=tool_name,
                                toolCallId=tool_call_id,
                                arguments=parsed_args,
                            )
                            event_bus.emit(
                                ObservabilityEventType.TOOL_START,
                                toolCallId=tool_call_id,
                                toolName=tool_name,
                            )

                            tool_event = Event(
                                type=EventType.TOOL_CALL,
                                data={
                                    "id": tool_call_id,
                                    "name": tool_name,
                                    "arguments": tool_args,
                                },
                            )
                            _fire_callback(
                                cb.on_tool_call,
                                tool_name,
                                tool_call_id,
                                parsed_args,
                            )

                            # Emit TOOL_COMPLETED (tool call passed through)
                            event_bus.emit(
                                ObservabilityEventType.TOOL_COMPLETED,
                                toolCallId=tool_call_id,
                                status="success",
                            )

                            yield tool_event
                        tool_call_buffers.clear()

                    while True:
                        if aborted:
                            event_bus.emit(
                                ObservabilityEventType.ABORT_COMPLETED,
                                tokenCount=state.token_count,
                                contentLength=len(state.content),
                            )
                            return

                        # Determine which timeout to use
                        current_timeout = (
                            inter_timeout if first_token_received else initial_timeout
                        )

                        try:
                            if current_timeout is not None:
                                adapted_event = await asyncio.wait_for(
                                    adapted_stream.__anext__(), timeout=current_timeout
                                )
                            else:
                                adapted_event = await adapted_stream.__anext__()

                            # Unpack AdaptedEvent to get Event and raw chunk
                            event = adapted_event.event
                            if adapted_event.raw_chunk is not None:
                                raw_chunks.append(adapted_event.raw_chunk)
                        except StopAsyncIteration:
                            break
                        except asyncio.TimeoutError as e:
                            timeout_type = (
                                "inter_token"
                                if first_token_received
                                else "initial_token"
                            )
                            # current_timeout is guaranteed to be float here
                            # (asyncio.TimeoutError only raised if wait_for was called)
                            assert current_timeout is not None
                            # Map internal timeout type to spec enum
                            timeout_type_enum = (
                                "initial"
                                if timeout_type == "initial_token"
                                else "inter"
                            )
                            # Use current_timeout which we've already asserted is not None
                            configured_ms = int(current_timeout * 1000)
                            event_bus.emit(
                                ObservabilityEventType.TIMEOUT_TRIGGERED,
                                timeoutType=timeout_type_enum,
                                elapsedMs=int(current_timeout * 1000),
                                configuredMs=configured_ms,
                            )
                            # Fire on_timeout callback
                            _fire_callback(cb.on_timeout, timeout_type, current_timeout)
                            token_desc = "next" if first_token_received else "first"
                            raise TimeoutError(
                                f"Timeout waiting for {token_desc} token "
                                f"(timeout={current_timeout}s)",
                                timeout_type=timeout_type,
                                timeout_seconds=current_timeout,
                            ) from e

                        if event.type == EventType.TOKEN and event.text:
                            token_text = event.text

                            # Handle deduplication for continuation
                            if (
                                state.continuation_used
                                and not state.deduplication_applied
                                and continuation_config
                                and continuation_config.deduplicate
                                and state.resume_point
                            ):
                                # Check for overlap with checkpoint
                                overlap_result = detect_overlap(
                                    state.resume_point,
                                    token_text,
                                    continuation_config.deduplication_options,
                                )

                                if overlap_result.has_overlap:
                                    token_text = overlap_result.deduplicated
                                    state.deduplication_applied = True
                                    state.overlap_removed = overlap_result.overlap_text
                                    logger.debug(
                                        f"Deduplication removed {overlap_result.overlap_length} chars overlap"
                                    )

                                    # Update the event with deduplicated text
                                    if not token_text:
                                        # Entire token was overlap, skip it
                                        continue
                                    event = Event(type=EventType.TOKEN, text=token_text)
                                else:
                                    state.deduplication_applied = (
                                        True  # Mark as checked
                                    )

                            if not first_token_received and inter_timeout is not None:
                                # First token received, switch to inter-token timeout
                                event_bus.emit(
                                    ObservabilityEventType.TIMEOUT_RESET,
                                    timeoutType="inter",
                                    configuredMs=int(inter_timeout * 1000),
                                    tokenIndex=state.token_count,
                                )
                            first_token_received = True
                            append_token(state, token_text)

                            # Save checkpoint periodically (only when continuation is enabled)
                            if (
                                state.token_count % checkpoint_interval == 0
                                and continuation_config is not None
                            ):
                                update_checkpoint(state)
                                # Emit CHECKPOINT_SAVED event
                                event_bus.emit(
                                    ObservabilityEventType.CHECKPOINT_SAVED,
                                    checkpoint=state.checkpoint,
                                    tokenCount=state.token_count,
                                )
                                # Fire on_checkpoint callback
                                _fire_callback(
                                    cb.on_checkpoint,
                                    state.checkpoint,
                                    state.token_count,
                                )

                            # Fire per-token callbacks (skip function call overhead when None)
                            if cb.on_event is not None:
                                _fire_callback(cb.on_event, event)
                            if cb.on_token is not None:
                                _fire_callback(cb.on_token, token_text)

                            # Check guardrails periodically
                            if (
                                state.token_count % guardrail_interval == 0
                                and guardrails
                            ):
                                flush_content(state)
                                _has_obs = event_bus._handler is not None

                                if _has_obs:
                                    phase_start_time = time.perf_counter()
                                    event_bus.emit(
                                        ObservabilityEventType.GUARDRAIL_PHASE_START,
                                        phase="post",
                                        ruleCount=len(guardrails),
                                    )

                                all_violations = []
                                for idx, rule in enumerate(guardrails):
                                    if _has_obs:
                                        callback_id = _next_callback_id()
                                        rule_start_time = time.perf_counter()
                                        event_bus.emit(
                                            ObservabilityEventType.GUARDRAIL_RULE_START,
                                            index=idx,
                                            ruleId=rule.name,
                                            callbackId=callback_id,
                                        )

                                    rule_violations = rule.check(state)

                                    if _has_obs:
                                        passed = len(rule_violations) == 0
                                        rule_duration_ms = int(
                                            (time.perf_counter() - rule_start_time) * 1000
                                        )
                                        event_bus.emit(
                                            ObservabilityEventType.GUARDRAIL_RULE_RESULT,
                                            index=idx,
                                            ruleId=rule.name,
                                            passed=passed,
                                            violation=rule_violations[0].__dict__
                                            if rule_violations
                                            else None,
                                        )

                                    if rule_violations:
                                        all_violations.extend(rule_violations)

                                    if _has_obs:
                                        event_bus.emit(
                                            ObservabilityEventType.GUARDRAIL_RULE_END,
                                            index=idx,
                                            ruleId=rule.name,
                                            passed=passed,
                                            callbackId=callback_id,
                                            durationMs=rule_duration_ms,
                                        )

                                if all_violations:
                                    state.violations.extend(all_violations)
                                    # Fire on_violation callback for each violation
                                    for v in all_violations:
                                        _fire_callback(cb.on_violation, v)

                                if _has_obs:
                                    phase_duration_ms = int(
                                        (time.perf_counter() - phase_start_time) * 1000
                                    )
                                    event_bus.emit(
                                        ObservabilityEventType.GUARDRAIL_PHASE_END,
                                        phase="post",
                                        passed=len(all_violations) == 0,
                                        violations=[v.__dict__ for v in all_violations],
                                        durationMs=phase_duration_ms,
                                    )

                            # Check drift periodically
                            if (
                                drift_detector is not None
                                and state.token_count % drift_interval == 0
                            ):
                                flush_content(state)
                                drift_result = drift_detector.check(
                                    state.content, token_text
                                )
                                if drift_result.detected:
                                    state.drift_detected = True
                                    event_bus.emit(
                                        ObservabilityEventType.DRIFT_CHECK_RESULT,
                                        detected=True,
                                        score=drift_result.confidence,
                                        metrics={t: True for t in drift_result.types},
                                        threshold=0.5,
                                    )
                                    # Fire on_drift callback
                                    _fire_callback(
                                        cb.on_drift,
                                        drift_result.types,
                                        drift_result.confidence,
                                    )
                                else:
                                    event_bus.emit(
                                        ObservabilityEventType.DRIFT_CHECK_RESULT,
                                        detected=False,
                                        score=0.0,
                                        metrics={},
                                        threshold=0.5,
                                    )
                        # Handle tool call buffering
                        if buffer_tool_calls and event.type == EventType.TOOL_CALL:
                            # Buffer tool call data by index
                            tc_data = event.data or {}
                            # Use 'index' if provided, otherwise use length as index
                            tc_index = tc_data.get("index", len(tool_call_buffers))

                            if tc_index not in tool_call_buffers:
                                # New tool call - initialize buffer
                                tool_call_buffers[tc_index] = {
                                    "id": tc_data.get("id"),
                                    "name": tc_data.get("name"),
                                    "arguments": tc_data.get("arguments") or "",
                                }
                            else:
                                # Existing tool call - accumulate arguments
                                buffered = tool_call_buffers[tc_index]
                                # Update id/name if provided (first chunk has these)
                                if tc_data.get("id"):
                                    buffered["id"] = tc_data["id"]
                                if tc_data.get("name"):
                                    buffered["name"] = tc_data["name"]
                                # Append arguments
                                if tc_data.get("arguments"):
                                    buffered["arguments"] += tc_data["arguments"]

                            # Don't yield partial tool calls - they'll be emitted
                            # when stream ends
                            continue

                        # Emit TOOL_* events for non-buffered tool calls
                        if not buffer_tool_calls and event.type == EventType.TOOL_CALL:
                            tc_data = event.data or {}
                            tool_call_id = tc_data.get("id", "")
                            tool_name = tc_data.get("name", "")

                            # Only emit TOOL_REQUESTED/TOOL_START once per tool call
                            if tool_call_id and tool_call_id not in tool_calls_started:
                                tool_calls_started.add(tool_call_id)
                                # Parse arguments to dict for event emission
                                raw_args = tc_data.get("arguments", "")
                                parsed_tc_args: dict[str, Any] = {}
                                if raw_args:
                                    import json

                                    try:
                                        parsed_tc_args = json.loads(raw_args)
                                    except (json.JSONDecodeError, TypeError):
                                        parsed_tc_args = {"_raw": raw_args}
                                event_bus.emit(
                                    ObservabilityEventType.TOOL_REQUESTED,
                                    toolName=tool_name,
                                    toolCallId=tool_call_id,
                                    arguments=parsed_tc_args,
                                )
                                event_bus.emit(
                                    ObservabilityEventType.TOOL_START,
                                    toolCallId=tool_call_id,
                                    toolName=tool_name,
                                )
                                # Fire on_tool_call callback with parsed arguments
                                _fire_callback(
                                    cb.on_tool_call,
                                    tool_name,
                                    tool_call_id,
                                    parsed_tc_args,
                                )

                        yield event

                    # Emit TOOL_COMPLETED for non-buffered tool calls at end of stream
                    for tool_call_id in tool_calls_started:
                        event_bus.emit(
                            ObservabilityEventType.TOOL_COMPLETED,
                            toolCallId=tool_call_id,
                            status="success",
                        )

                    # Emit any buffered tool calls at end of stream
                    if buffer_tool_calls and tool_call_buffers:
                        async for tc_event in emit_buffered_tool_calls():
                            yield tc_event

                    # Success
                    mark_completed(state)

                    # Run final guardrail check (for completion-only rules)
                    if guardrails:
                        final_phase_start_time = time.perf_counter()
                        event_bus.emit(
                            ObservabilityEventType.GUARDRAIL_PHASE_START,
                            phase="post",
                            ruleCount=len(guardrails),
                        )

                        all_violations = []
                        for idx, rule in enumerate(guardrails):
                            callback_id = _next_callback_id()
                            rule_start_time = time.perf_counter()
                            event_bus.emit(
                                ObservabilityEventType.GUARDRAIL_RULE_START,
                                index=idx,
                                ruleId=rule.name,
                                callbackId=callback_id,
                            )
                            rule_violations = rule.check(state)
                            passed = len(rule_violations) == 0
                            rule_duration_ms = int(
                                (time.perf_counter() - rule_start_time) * 1000
                            )
                            event_bus.emit(
                                ObservabilityEventType.GUARDRAIL_RULE_RESULT,
                                index=idx,
                                ruleId=rule.name,
                                passed=passed,
                                violation=rule_violations[0].__dict__
                                if rule_violations
                                else None,
                            )
                            if rule_violations:
                                all_violations.extend(rule_violations)
                            event_bus.emit(
                                ObservabilityEventType.GUARDRAIL_RULE_END,
                                index=idx,
                                ruleId=rule.name,
                                passed=passed,
                                callbackId=callback_id,
                                durationMs=rule_duration_ms,
                            )

                        if all_violations:
                            state.violations.extend(all_violations)
                            # Fire on_violation callback for final violations
                            for v in all_violations:
                                _fire_callback(cb.on_violation, v)

                        final_phase_duration_ms = int(
                            (time.perf_counter() - final_phase_start_time) * 1000
                        )
                        event_bus.emit(
                            ObservabilityEventType.GUARDRAIL_PHASE_END,
                            phase="post",
                            passed=len(all_violations) == 0,
                            violations=[v.__dict__ for v in all_violations],
                            durationMs=final_phase_duration_ms,
                        )

                        # Fatal violations (non-recoverable errors) halt completely
                        # Check these first to avoid triggering retries that are doomed to fail
                        fatal_violations = [
                            v
                            for v in all_violations
                            if v.severity == "error" and not v.recoverable
                        ]
                        if fatal_violations:
                            first_violation = fatal_violations[0]
                            raise Error(
                                f"Fatal guardrail violation: {first_violation.message}",
                                ErrorContext(
                                    code=ErrorCode.FATAL_GUARDRAIL_VIOLATION,
                                    checkpoint=state.checkpoint,
                                    token_count=state.token_count,
                                    content_length=len(state.content),
                                    model_retry_count=retry_mgr.model_retry_count,
                                    network_retry_count=retry_mgr.network_retry_count,
                                    fallback_index=fallback_idx,
                                    context=context,
                                ),
                            )

                        # Check for violations that should trigger retry
                        # Error-severity violations with recoverable=True trigger retry
                        error_violations = [
                            v
                            for v in all_violations
                            if v.severity == "error" and v.recoverable
                        ]
                        if error_violations:
                            first_violation = error_violations[0]
                            raise Error(
                                f"Guardrail violation: {first_violation.message}",
                                ErrorContext(
                                    code=ErrorCode.GUARDRAIL_VIOLATION,
                                    checkpoint=state.checkpoint,
                                    token_count=state.token_count,
                                    content_length=len(state.content),
                                    model_retry_count=retry_mgr.model_retry_count,
                                    network_retry_count=retry_mgr.network_retry_count,
                                    fallback_index=fallback_idx,
                                    context=context,
                                ),
                            )

                    if fallback_idx > 0:
                        event_bus.emit(
                            ObservabilityEventType.FALLBACK_END, index=fallback_idx
                        )
                    event_bus.emit(
                        ObservabilityEventType.COMPLETE,
                        tokenCount=state.token_count,
                        contentLength=len(state.content),
                    )

                    # Fire on_complete callback
                    _fire_callback(cb.on_complete, state)

                    logger.debug(f"Stream complete: {state.token_count} tokens")
                    return

                except Exception as e:
                    errors.append(e)
                    logger.debug(f"Stream error: {e}")

                    # Determine error category
                    from .errors import analyze_network_error, categorize_error
                    from .types import ErrorCategory

                    error_category = categorize_error(e)
                    is_network = error_category == ErrorCategory.NETWORK

                    # Emit NETWORK_ERROR event if this is a network error
                    if is_network:
                        network_analysis = analyze_network_error(e)
                        event_bus.emit(
                            ObservabilityEventType.NETWORK_ERROR,
                            error=str(e),
                            code=network_analysis.type
                            if network_analysis
                            else "unknown",
                            retryable=network_analysis.retryable
                            if network_analysis
                            else True,
                        )

                    # Determine if we will retry or fallback
                    # First check basic retry conditions (sync)
                    default_should_retry = retry_mgr.should_retry(e)

                    # If custom should_retry callback exists, call it with events
                    will_retry = default_should_retry
                    if (
                        default_should_retry
                        and retry_mgr.config.should_retry is not None
                    ):
                        attempt = (
                            retry_mgr.network_retry_count
                            if error_category
                            in (ErrorCategory.NETWORK, ErrorCategory.TRANSIENT)
                            else retry_mgr.model_retry_count
                        )

                        # Emit RETRY_FN_START
                        event_bus.emit(
                            ObservabilityEventType.RETRY_FN_START,
                            attempt=attempt,
                            category=error_category.value,
                            defaultShouldRetry=default_should_retry,
                        )

                        fn_start_time = time.perf_counter()
                        try:
                            # Call the callback (may be sync or async)
                            user_result = retry_mgr.config.should_retry(
                                e, state, attempt, error_category
                            )
                            if inspect.iscoroutine(user_result):
                                user_result = await user_result

                            duration_ms = int(
                                (time.perf_counter() - fn_start_time) * 1000
                            )

                            # User can only veto (narrow), not force (widen)
                            final_should_retry = default_should_retry and bool(
                                user_result
                            )

                            # Emit RETRY_FN_RESULT
                            event_bus.emit(
                                ObservabilityEventType.RETRY_FN_RESULT,
                                attempt=attempt,
                                category=error_category.value,
                                userResult=bool(user_result),
                                finalShouldRetry=final_should_retry,
                                durationMs=duration_ms,
                            )

                            will_retry = final_should_retry
                        except Exception as fn_error:
                            duration_ms = int(
                                (time.perf_counter() - fn_start_time) * 1000
                            )
                            fn_err_msg = str(fn_error)

                            # Emit RETRY_FN_ERROR - exception treated as veto
                            event_bus.emit(
                                ObservabilityEventType.RETRY_FN_ERROR,
                                attempt=attempt,
                                category=error_category.value,
                                error=fn_err_msg,
                                finalShouldRetry=False,
                                durationMs=duration_ms,
                            )

                            will_retry = False

                    will_fallback = not will_retry and fallback_idx < len(streams) - 1

                    # Fire on_error callback BEFORE retry/fallback decision
                    _fire_callback(cb.on_error, e, will_retry, will_fallback)

                    # Build recovery policy (matches TS)
                    error_code = getattr(e, "code", None)
                    event_bus.emit(
                        ObservabilityEventType.ERROR,
                        error=str(e),
                        errorCode=error_code.value if error_code else None,
                        failureType=_get_failure_type(e),
                        recoveryStrategy=_get_recovery_strategy(will_retry, will_fallback),
                        policy={
                            "retryEnabled": retry_mgr.config.max_retries > 0,
                            "fallbackEnabled": len(streams) > 1,
                            "maxRetries": retry_mgr.config.max_retries,
                            "maxFallbacks": len(streams) - 1,
                            "attempt": attempt_number,
                            "fallbackIndex": fallback_idx,
                        },
                    )

                    if will_retry:
                        event_bus.emit(
                            ObservabilityEventType.RETRY_START,
                            attempt=retry_mgr.total_retries + 1,
                            maxAttempts=retry_mgr.config.max_retries,
                        )
                        retry_mgr.record_attempt(e)
                        state.model_retry_count = retry_mgr.model_retry_count
                        state.network_retry_count = retry_mgr.network_retry_count

                        # Fire on_retry callback
                        _fire_callback(cb.on_retry, retry_mgr.total_retries, str(e))

                        event_bus.emit(
                            ObservabilityEventType.RETRY_ATTEMPT,
                            attempt=retry_mgr.total_retries,
                            reason=str(e),
                            isNetwork=is_network,
                        )
                        await retry_mgr.wait(e)

                        # Save checkpoint for potential continuation
                        update_checkpoint(state)
                        event_bus.emit(
                            ObservabilityEventType.CHECKPOINT_SAVED,
                            checkpoint=state.checkpoint,
                            tokenCount=state.token_count,
                        )
                        # Fire on_checkpoint callback
                        _fire_callback(
                            cb.on_checkpoint, state.checkpoint, state.token_count
                        )

                        # Enable continuation on next retry if configured
                        if continuation_config and state.checkpoint:
                            use_continuation_on_retry = True
                            pending_checkpoint = state.checkpoint
                        else:
                            # Reset state for fresh retry (no continuation)
                            state.content = ""
                            state._content_buffer.clear()
                            state.token_count = 0
                            state.checkpoint = ""
                            state.completed = False
                            state.violations = []

                        state.resumed = True
                        event_bus.emit(ObservabilityEventType.RETRY_END, success=False)
                        continue
                    else:
                        # Try next fallback
                        event_bus.emit(
                            ObservabilityEventType.FALLBACK_END, index=fallback_idx
                        )
                        break

        # All fallbacks exhausted
        event_bus.emit(
            ObservabilityEventType.RETRY_GIVE_UP,
            attempts=retry_mgr.total_retries,
            lastError=str(errors[-1]) if errors else None,
        )
        if errors:
            raise errors[-1]
        raise RuntimeError("All streams and fallbacks exhausted")

    return Stream(
        iterator=run_stream(),
        state=state,
        abort=abort,
        errors=errors,
        raw_chunks=raw_chunks,
    )
