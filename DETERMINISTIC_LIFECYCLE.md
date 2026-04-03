# L0 Deterministic Lifecycle Specification

This document specifies the **deterministic lifecycle behavior** of the L0 Python runtime. It serves as a reference for understanding the execution flow, event ordering, and callback behavior.

## Deterministic Lifecycle Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            L0 LIFECYCLE FLOW                                │
└─────────────────────────────────────────────────────────────────────────────┘

                                ┌──────────┐
                                │  START   │
                                └────┬─────┘
                                     │
                                     ▼
                      ┌──────────────────────────────────┐
                      │ on_start(attempt, false, false)  │
                      └──────────────┬───────────────────┘
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              STREAMING PHASE                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         on_event(event)                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  During streaming, these callbacks fire as conditions occur:               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │on_checkpoint │  │ on_tool_call │  │   on_drift   │  │  on_timeout  │   │
│  │ (checkpoint, │  │ (tool_name,  │  │ (types,      │  │ (type,       │   │
│  │  token_count)│  │  id, args)   │  │  confidence) │  │  elapsed_sec)│   │
│  └──────────────┘  └──────────────┘  └──────┬───────┘  └──────┬───────┘   │
│                                             │                  │           │
│                                             └────────┬─────────┘           │
│                                                      │ triggers retry      │
└──────────────────────────────────────────────────────┼─────────────────────┘
                                                       │
              ┌────────────────────────────────────────┼────────────────┐
              │                    │                   │                │
              ▼                    ▼                   ▼                ▼
        ┌─────────┐          ┌───────────┐                        ┌─────────┐
        │ SUCCESS │          │   ERROR   │                        │  ABORT  │
        └────┬────┘          └─────┬─────┘                        └────┬────┘
             │                     │                                   │
             │     During STREAMING/FINALIZING, violations fire        ▼
             │     on_violation(violation) as an event within     ┌───────────┐
             │     the current state (no separate VIOLATION state) │ on_abort  │
             │                     │                              │(token_cnt,│
             │                     ▼                              │content_len)│
             │              ┌────────────────────────────────┐    └───────────┘
             │              │ on_error(error, will_retry,    │
             │              │         will_fallback)         │
             │              └──────────────┬─────────────────┘
             │                             │
             │                 ┌───────────┼───────────┐
             │                 │           │           │
             │                 ▼           ▼           ▼
             │           ┌──────────┐ ┌──────────┐ ┌──────────┐
             │           │  RETRY   │ │ FALLBACK │ │  FATAL   │
             │           └────┬─────┘ └────┬─────┘ └────┬─────┘
             │                │            │            │
             │                ▼            ▼            │
             │          ┌───────────┐ ┌───────────┐     │
             │          │ on_retry  │ │on_fallback│     │
             │          └─────┬─────┘ └─────┬─────┘     │
             │                │             │           │
             │                │    ┌────────┘           │
             │                │    │                    │
             │                ▼    ▼                    │
             │          ┌─────────────────────┐         │
             │          │  Has checkpoint?    │         │
             │          └──────────┬──────────┘         │
             │                YES  │  NO                │
             │                ┌────┴────┐               │
             │                ▼         ▼               │
             │          ┌──────────┐    │               │
             │          │ on_resume│    │               │
             │          └────┬─────┘    │               │
             │               │          │               │
             │               ▼          ▼               │
             │          ┌─────────────────────────┐     │
             │          │on_start(attempt, is_retry,│   │
             │          │        is_fallback)      │────┼──► Back to STREAMING
             │          └─────────────────────────┘     │
             │                                          │
             ▼                                          ▼
      ┌─────────────┐                            ┌──────────┐
      │ on_complete │                            │  THROW   │
      │   (state)   │                            │  ERROR   │
      └─────────────┘                            └──────────┘
```

## Event Ordering Specifications

**Important:**

- `SESSION_START` is emitted exactly ONCE at the beginning of the session (anchor for entire session).
- `ATTEMPT_START` is emitted for each retry attempt.
- `FALLBACK_START` is emitted when switching to a fallback stream.
- The `on_start` callback fires for `SESSION_START` (initial), `ATTEMPT_START` (retries), and `FALLBACK_START` (fallbacks).

### Normal Successful Flow

```
1. SESSION_START (attempt=1, isRetry=false, isFallback=false) → on_start(1, False, False)
2. STREAM_INIT
3. ADAPTER_WRAP_START
4. ADAPTER_DETECTED (adapterId="openai" or "litellm")
5. STREAM_READY
6. ADAPTER_WRAP_END
7. TIMEOUT_START (if timeout configured)
8. [tokens stream...] → on_token(text), on_event(event)
9. CHECKPOINT_SAVED (if continuation enabled, every N tokens) → on_checkpoint(checkpoint, token_count)
10. COMPLETE (with full State) → on_complete(state)
```

### Retry Flow (guardrail violation, drift, network error)

```
1. SESSION_START (attempt=1, isRetry=false, isFallback=false) → on_start(1, False, False)
2. STREAM_INIT, STREAM_READY
3. [tokens stream...]
4. ERROR detected
5. NETWORK_ERROR (if network error)
6. on_error(error, will_retry=True, will_fallback=False)
7. RETRY_START
8. RETRY_FN_START (if custom should_retry callback)
9. RETRY_FN_RESULT (callback result)
10. RETRY_ATTEMPT (attempt=N, reason)
11. on_retry(attempt, reason)
12. ATTEMPT_START (attempt=2, isRetry=true, isFallback=false) → on_start(2, True, False)
13. [tokens stream...]
14. RETRY_END (success=True)
15. COMPLETE → on_complete(state)
```

### Fallback Flow (retries exhausted)

```
1. SESSION_START (attempt=1, isRetry=false, isFallback=false) → on_start(1, False, False)
2. [error occurs, retries exhausted]
3. RETRY_GIVE_UP
4. on_error(error, will_retry=False, will_fallback=True)
5. FALLBACK_START (fromIndex=0, toIndex=1, reason="previous_failed")
6. FALLBACK_MODEL_SELECTED (index=1)
7. on_fallback(0, "previous_failed")  # 0-based fallback index
8. on_start(1, False, True)
9. STREAM_INIT, STREAM_READY
10. [tokens stream...]
11. FALLBACK_END (index=1)
12. COMPLETE → on_complete(state)
```

### Continuation/Resume Flow

```
1. SESSION_START (attempt=1) → on_start(1, False, False)
2. [tokens stream...]
3. CHECKPOINT_SAVED → on_checkpoint(checkpoint, token_count)
4. [error occurs]
5. ERROR
6. on_error(error, will_retry=True, will_fallback=False)
7. RETRY_ATTEMPT + ATTEMPT_START → on_start(N, True, False)
   or FALLBACK_START → on_start(1, False, True)
8. CONTINUATION_START (checkpointLength=N)
9. RESUME_START (checkpoint=content, tokenCount=N) → on_resume(checkpoint, token_count)
10. [continuation tokens...]
11. COMPLETE → on_complete(state)
```

### Abort Flow

```
1. SESSION_START → on_start(1, False, False)
2. [tokens stream...]
3. [stream.abort() called]
4. ABORT_REQUESTED (source="user") → on_abort(token_count, content_length)
5. ABORT_COMPLETED (tokenCount, contentLength)
6. [raises Error with code STREAM_ABORTED]
```

### Timeout Flow

```
1. SESSION_START → on_start(1, False, False)
2. TIMEOUT_START (timeoutType="initial", configuredMs=N)
3. [waiting for token...]
4. TIMEOUT_TRIGGERED (timeoutType="initial" or "inter", elapsedMs, configuredMs)
5. on_timeout(timeout_type, elapsed_seconds)
6. ERROR
7. on_error(error, will_retry=True/False, will_fallback=True/False)
8. [retry or fallback flow...]
```

### Guardrail Violation Flow

```
1. SESSION_START → on_start(1, False, False)
2. [tokens stream...]
3. GUARDRAIL_PHASE_START (phase="post", ruleCount=N)
4. GUARDRAIL_RULE_START (index=0, ruleId="rule_name", callbackId="cb_...")
5. GUARDRAIL_RULE_RESULT (index=0, ruleId="rule_name", passed=False, violation={...})
6. on_violation(violation)
7. GUARDRAIL_RULE_END (index=0, ruleId="rule_name", passed=False, durationMs=N)
8. GUARDRAIL_PHASE_END (phase="post", passed=False, violations=[...], durationMs=N)
9. ERROR (for error-severity violations)
10. on_error(error, will_retry=True/False, will_fallback=True/False)
11. [retry or fallback flow...]
```

## Callback Signatures

All callbacks are defined in the `LifecycleCallbacks` dataclass:

```python
from l0 import LifecycleCallbacks

callbacks = LifecycleCallbacks(
    on_start=lambda attempt, is_retry, is_fallback: ...,
    on_complete=lambda state: ...,
    on_error=lambda error, will_retry, will_fallback: ...,
    on_event=lambda event: ...,
    on_violation=lambda violation: ...,
    on_retry=lambda attempt, reason: ...,
    on_fallback=lambda index, reason: ...,
    on_resume=lambda checkpoint, token_count: ...,
    on_checkpoint=lambda checkpoint, token_count: ...,
    on_timeout=lambda timeout_type, elapsed_seconds: ...,
    on_abort=lambda token_count, content_length: ...,
    on_drift=lambda drift_types, confidence: ...,
    on_tool_call=lambda tool_name, tool_call_id, args: ...,
    on_token=lambda text: ...,
)
```

| Callback        | Signature                                                             | When Called                            |
| --------------- | --------------------------------------------------------------------- | -------------------------------------- |
| `on_start`      | `(attempt: int, is_retry: bool, is_fallback: bool) -> None`           | New execution attempt begins           |
| `on_complete`   | `(state: State) -> None`                                              | Stream finished successfully           |
| `on_error`      | `(error: Exception, will_retry: bool, will_fallback: bool) -> None`   | Error occurred (before retry decision) |
| `on_event`      | `(event: Event) -> None`                                              | Any streaming event emitted            |
| `on_violation`  | `(violation: GuardrailViolation) -> None`                             | Guardrail violation detected           |
| `on_retry`      | `(attempt: int, reason: str) -> None`                                 | Retry triggered (same model)           |
| `on_fallback`   | `(index: int, reason: str) -> None`                                   | Switching to fallback model            |
| `on_resume`     | `(checkpoint: str, token_count: int) -> None`                         | Continuing from checkpoint             |
| `on_checkpoint` | `(checkpoint: str, token_count: int) -> None`                         | Checkpoint saved                       |
| `on_timeout`    | `(timeout_type: str, elapsed_seconds: float) -> None`                 | Timeout occurred                       |
| `on_abort`      | `(token_count: int, content_length: int) -> None`                     | Stream was aborted                     |
| `on_drift`      | `(drift_types: list[str], confidence: float \| None) -> None`         | Semantic drift detected                |
| `on_tool_call`  | `(tool_name: str, tool_call_id: str, args: dict[str, Any]) -> None`   | Tool call detected in stream           |
| `on_token`      | `(text: str) -> None`                                                 | Token received                         |

### Callback Behavior

All callbacks are **fire-and-forget**:
- They never block the stream
- Errors in callbacks are silently caught and logged at debug level
- Callbacks are optional - omit any you don't need

```python
# Using individual callback parameters
result = await l0.run(
    stream=my_stream,
    on_start=lambda a, r, f: print(f"Attempt {a}"),
    on_complete=lambda s: print(f"Done: {len(s.content)} chars"),
    on_error=lambda e, r, f: print(f"Error: {e}"),
)

# Using LifecycleCallbacks object
from l0 import LifecycleCallbacks

callbacks = LifecycleCallbacks(
    on_start=lambda a, r, f: print(f"Attempt {a}"),
    on_complete=lambda s: print(f"Done: {len(s.content)} chars"),
)

result = await l0.run(
    stream=my_stream,
    callbacks=callbacks,
)
```

## Parameter Indexing

### 1-Based Parameters (Human-Friendly)

These parameters use 1-based indexing for human readability:

- **`on_start` → `attempt`**: First attempt is `1`, second is `2`, etc.
- **`on_retry` → `attempt`**: The retry attempt number (1-based)

### 0-Based Parameters (Programmer-Friendly)

These parameters use 0-based indexing for array/iteration compatibility:

- **`on_fallback` → `index`**: First fallback is `0`, second is `1`, etc.
- **`should_retry` → `attempt`**: Current attempt (0-based) for retry veto decisions
- **`calculate_delay` context → `attempt`**: Used for delay calculations

## Observability Events

The following `ObservabilityEventType` values are emitted during the lifecycle:

### Session & Stream Events

| Event Type        | Description                           | Meta Fields                          |
| ----------------- | ------------------------------------- | ------------------------------------ |
| `SESSION_START`   | Session started (once per session)    | `attempt`, `isRetry`, `isFallback`   |
| `ATTEMPT_START`   | New attempt started (retry)           | `attempt`, `isFallback`              |
| `STREAM_INIT`     | Stream initialization started         | -                                    |
| `STREAM_READY`    | Stream ready to consume               | -                                    |
| `TOKEN`           | Token received                        | `text`                               |
| `FINALIZATION_START` | Finalization phase started         | -                                    |
| `FINALIZATION_END`   | Finalization phase ended           | -                                    |
| `COMPLETE`        | Stream completed successfully         | `tokenCount`, `contentLength`        |
| `ERROR`           | Error occurred                        | `error`, `code`, `recoveryStrategy`  |
| `SESSION_END`     | Session ended                         | `success`, `totalAttempts`           |
| `SESSION_SUMMARY` | Summary of session                    | `duration`, `tokenCount`, etc.       |

### Adapter Events

| Event Type           | Description                              | Meta Fields   |
| -------------------- | ---------------------------------------- | ------------- |
| `ADAPTER_WRAP_START` | Adapter wrapping started                 | -             |
| `ADAPTER_DETECTED`   | Adapter detected                         | `adapterId`   |
| `ADAPTER_WRAP_END`   | Adapter wrapping completed               | -             |

### Timeout Events

| Event Type          | Description                                    | Meta Fields                         |
| ------------------- | ---------------------------------------------- | ----------------------------------- |
| `TIMEOUT_START`     | Timeout timer started                          | `timeoutType`, `configuredMs`       |
| `TIMEOUT_RESET`     | Timeout timer reset after token                | `timeoutType`, `configuredMs`, `tokenIndex` |
| `TIMEOUT_TRIGGERED` | Timeout occurred                               | `timeoutType`, `elapsedMs` (configured timeout value in ms), `configuredMs` |

### Network Events

| Event Type           | Description                | Meta Fields                    |
| -------------------- | -------------------------- | ------------------------------ |
| `NETWORK_ERROR`      | Network error occurred     | `error`, `code`, `retryable`   |
| `NETWORK_RECOVERY`   | Recovered from network err | -                              |
| `CONNECTION_DROPPED` | Connection dropped         | -                              |
| `CONNECTION_RESTORED`| Connection restored        | -                              |

### Retry Events

| Event Type       | Description                                       | Meta Fields                                      |
| ---------------- | ------------------------------------------------- | ------------------------------------------------ |
| `RETRY_START`    | Retry sequence starting                           | `maxAttempts`, `category`                        |
| `RETRY_ATTEMPT`  | Individual retry attempt                          | `attempt`, `reason`, `delayMs`                   |
| `RETRY_END`      | Retry succeeded                                   | `success`, `attempts`                            |
| `RETRY_GIVE_UP`  | All retries exhausted                             | `attempts`, `lastError`                          |
| `RETRY_FN_START` | Custom should_retry callback starting             | `attempt`, `category`, `defaultShouldRetry`      |
| `RETRY_FN_RESULT`| Custom should_retry callback result               | `attempt`, `category`, `userResult`, `finalShouldRetry`, `durationMs` |
| `RETRY_FN_ERROR` | Custom should_retry callback threw                | `attempt`, `category`, `error`, `durationMs`     |

### Fallback Events

| Event Type                | Description                              | Meta Fields            |
| ------------------------- | ---------------------------------------- | ---------------------- |
| `FALLBACK_START`          | Switching to fallback stream             | `fromIndex`, `toIndex`, `reason` |
| `FALLBACK_MODEL_SELECTED` | Fallback model selected                  | `index`                |
| `FALLBACK_END`            | Fallback completed                       | `index`, `success`     |

### Continuation Events

| Event Type           | Description                   | Meta Fields                    |
| -------------------- | ----------------------------- | ------------------------------ |
| `CONTINUATION_START` | Continuing from checkpoint    | `checkpointLength`             |
| `CHECKPOINT_SAVED`   | Checkpoint was saved          | `checkpoint`, `tokenCount`     |
| `RESUME_START`       | Resuming from checkpoint      | `checkpoint`, `tokenCount`     |

### Abort Events

| Event Type        | Description          | Meta Fields                       |
| ----------------- | -------------------- | --------------------------------- |
| `ABORT_REQUESTED` | Abort was requested  | `source`                          |
| `ABORT_COMPLETED` | Abort completed      | `tokenCount`, `contentLength`     |

### Tool Events

| Event Type       | Description                    | Meta Fields                           |
| ---------------- | ------------------------------ | ------------------------------------- |
| `TOOL_REQUESTED` | Tool call requested            | `toolName`, `toolCallId`, `arguments` |
| `TOOL_START`     | Tool execution started         | `toolCallId`, `toolName`              |
| `TOOL_RESULT`    | Tool returned result           | `toolCallId`, `result`                |
| `TOOL_ERROR`     | Tool execution failed          | `toolCallId`, `error`                 |
| `TOOL_COMPLETED` | Tool call completed            | `toolCallId`, `status`                |

### Guardrail Events

| Event Type               | Description                           | Meta Fields                              |
| ------------------------ | ------------------------------------- | ---------------------------------------- |
| `GUARDRAIL_PHASE_START`  | Guardrail phase starting              | `phase`, `ruleCount`                     |
| `GUARDRAIL_RULE_START`   | Individual rule starting              | `index`, `ruleId`, `callbackId`          |
| `GUARDRAIL_RULE_RESULT`  | Rule evaluation result                | `index`, `ruleId`, `passed`, `violation` |
| `GUARDRAIL_RULE_END`     | Individual rule completed             | `index`, `ruleId`, `passed`, `callbackId`, `durationMs` |
| `GUARDRAIL_PHASE_END`    | Guardrail phase completed             | `phase`, `passed`, `violations`, `durationMs` |
| `GUARDRAIL_CALLBACK_START` | Guardrail callback starting         | `callbackId`                             |
| `GUARDRAIL_CALLBACK_END` | Guardrail callback completed          | `callbackId`, `durationMs`               |

### Drift Events

| Event Type            | Description              | Meta Fields                           |
| --------------------- | ------------------------ | ------------------------------------- |
| `DRIFT_CHECK_RESULT`  | Drift check completed    | `detected`, `score`, `metrics`, `threshold` |
| `DRIFT_CHECK_SKIPPED` | Drift check skipped      | `reason`                              |

### Structured Output Events

| Event Type                  | Description                      | Meta Fields              |
| --------------------------- | -------------------------------- | ------------------------ |
| `PARSE_START`                    | JSON parsing started                        | -                        |
| `PARSE_END`                      | JSON parsing completed                      | `durationMs`             |
| `PARSE_ERROR`                    | JSON parsing failed                         | `error`                  |
| `SCHEMA_VALIDATION_START`        | Schema validation started                   | -                        |
| `SCHEMA_VALIDATION_END`          | Schema validation completed                 | `durationMs`             |
| `SCHEMA_VALIDATION_ERROR`        | Schema validation failed                    | `error`                  |
| `AUTO_CORRECT_START`             | Auto-correction started                     | -                        |
| `AUTO_CORRECT_END`               | Auto-correction completed                   | `durationMs`, `fixed`    |
| `STRUCTURED_PARSE_START`         | JSON parsing started (TS compat alias)      | -                        |
| `STRUCTURED_PARSE_END`           | JSON parsing completed (TS compat alias)    | `durationMs`             |
| `STRUCTURED_PARSE_ERROR`         | JSON parsing failed (TS compat alias)       | `error`                  |
| `STRUCTURED_VALIDATION_START`    | Schema validation started (TS compat alias) | -                        |
| `STRUCTURED_VALIDATION_END`      | Schema validation completed (TS compat alias) | `durationMs`           |
| `STRUCTURED_VALIDATION_ERROR`    | Schema validation failed (TS compat alias)  | `error`                  |
| `STRUCTURED_AUTO_CORRECT_START`  | Auto-correction started (TS compat alias)   | -                        |
| `STRUCTURED_AUTO_CORRECT_END`    | Auto-correction completed (TS compat alias) | `durationMs`, `fixed`    |

### Consensus Events

| Event Type                 | Description                      | Meta Fields                    |
| -------------------------- | -------------------------------- | ------------------------------ |
| `CONSENSUS_START`          | Consensus operation started      | `streamCount`, `strategy`      |
| `CONSENSUS_STREAM_START`   | Individual stream started        | `index`                        |
| `CONSENSUS_STREAM_END`     | Individual stream ended          | `index`, `success`             |
| `CONSENSUS_OUTPUT_COLLECTED` | Output collected from stream   | `index`, `contentLength`       |
| `CONSENSUS_ANALYSIS`       | Consensus analysis completed     | `strategy`, `outputs`          |
| `CONSENSUS_RESOLUTION`     | Consensus resolved               | `result`, `confidence`         |
| `CONSENSUS_END`            | Consensus operation ended        | `success`, `durationMs`        |

## Implementation Notes

### Event Bus

All lifecycle events are emitted through a centralized `EventBus`. The bus:

- Assigns millisecond timestamps (`time.time() * 1000`) to all events
- Attaches a consistent `stream_id` (UUIDv7) across all events in a session
- Includes user-provided `context` dict in all observability events
- Silently catches handler errors (non-fatal)

```python
from l0.events import EventBus, ObservabilityEventType

def my_handler(event):
    print(f"[{event.type.value}] {event.meta}")

bus = EventBus(handler=my_handler, context={"request_id": "abc123"})
bus.emit(ObservabilityEventType.SESSION_START, attempt=1, isRetry=False, isFallback=False)
```

### State Machine

The runtime uses a `StateMachine` with these `RuntimeState` values and transitions:

- `INIT` → `WAITING_FOR_TOKEN` → `STREAMING`
- `STREAMING` → `TOOL_CALL_DETECTED` → `STREAMING`
- `STREAMING` → `CONTINUATION_MATCHING` → `CHECKPOINT_VERIFYING` → `STREAMING`
- `STREAMING` → `FINALIZING` → `COMPLETE`
- `STREAMING` → `RETRYING` → `STREAMING`
- `STREAMING` → `FALLBACK` → `STREAMING`
- `STREAMING` → `ERROR` (terminal)
- `FINALIZING` → `ERROR` (terminal)

Violations (guardrail failures) are handled as events **within** the `STREAMING` or `FINALIZING` states; there is no separate `VIOLATION` state.

### Callback Wrapper

The internal `_fire_callback` function ensures callbacks are fire-and-forget:

```python
def _fire_callback(callback: Callable[..., Any] | None, *args: Any) -> None:
    """Fire a callback without blocking or raising errors."""
    if callback is None:
        return
    try:
        callback(*args)
    except Exception as e:
        logger.debug(f"Callback error (silently caught): {e}")
```

## Usage Examples

### Full Lifecycle Tracking

```python
import l0
from l0 import LifecycleCallbacks

# Track all lifecycle events
callbacks = LifecycleCallbacks(
    on_start=lambda a, r, f: print(f"Start: attempt={a}, retry={r}, fallback={f}"),
    on_complete=lambda s: print(f"Complete: {s.token_count} tokens"),
    on_error=lambda e, r, f: print(f"Error: {e}, will_retry={r}, will_fallback={f}"),
    on_retry=lambda a, r: print(f"Retry: attempt={a}, reason={r}"),
    on_fallback=lambda i, r: print(f"Fallback: index={i}, reason={r}"),
    on_resume=lambda c, t: print(f"Resume: {t} tokens from checkpoint"),
    on_checkpoint=lambda c, t: print(f"Checkpoint: {t} tokens saved"),
    on_timeout=lambda t, e: print(f"Timeout: {t} after {e}s"),
    on_abort=lambda t, c: print(f"Abort: {t} tokens, {c} chars"),
    on_drift=lambda d, c: print(f"Drift: {d}, confidence={c}"),
    on_violation=lambda v: print(f"Violation: {v.message}"),
    on_tool_call=lambda n, i, a: print(f"Tool: {n}({a})"),
    on_token=lambda t: print(t, end="", flush=True),
)

result = await l0.run(
    stream=my_stream,
    callbacks=callbacks,
    retry=l0.Retry(max_attempts=3),
    continue_from_last_good_token=True,
)
```

### Observability Event Logging

```python
import l0
from l0.events import ObservabilityEvent

def log_event(event: ObservabilityEvent):
    print(f"[{event.ts}] {event.type.value}: {event.meta}")

result = await l0.run(
    stream=my_stream,
    on_event=log_event,
    context={"request_id": "req_123", "user_id": "user_456"},
)
```
