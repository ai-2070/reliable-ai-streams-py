# L0 API Reference

Complete API reference for L0 Python.

> Most applications should simply use `import l0`.
> See [Imports](#imports) for details on available exports.

## Table of Contents

- [Core Functions](#core-functions)
- [Lifecycle Callbacks](#lifecycle-callbacks)
- [Streaming Runtime](#streaming-runtime)
- [Retry Configuration](#retry-configuration)
- [Checkpoint Resumption](#checkpoint-resumption)
- [Smart Continuation Deduplication](#smart-continuation-deduplication)
- [Document Windows](#document-windows)
- [Pipeline](#pipeline)
- [Network Protection](#network-protection)
- [Structured Output](#structured-output)
- [Fallback Models](#fallback-models)
- [Guardrails](#guardrails)
- [Consensus](#consensus)
- [Parallel Operations](#parallel-operations)
- [Custom Adapters](#custom-adapters)
- [Observability](#observability)
- [Error Handling](#error-handling)
- [State Machine](#state-machine)
- [Metrics](#metrics)
- [Async Checks](#async-checks)
- [Formatting Helpers](#formatting-helpers)
- [Stream Utilities](#stream-utilities)
- [Utility Functions](#utility-functions)
- [Types](#types)
- [Imports](#imports)

---

## Core Functions

### wrap(client_or_stream, *, guardrails, retry, timeout, ...)

Wrap an OpenAI/LiteLLM client or raw stream with L0 reliability.

**This is the preferred API.** Pass a client for full retry support, or a raw stream for simple cases.

#### Wrapping a Client (Recommended)

```python
import l0
from openai import AsyncOpenAI

# Wrap the client once
client = l0.wrap(AsyncOpenAI())

# Use normally - L0 reliability is automatic
response = await client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True,
)

# Stream with L0 events
async for event in response:
    if event.is_token:
        print(event.text, end="")

# Or read all at once
text = await response.read()
```

#### With Full Configuration

```python
client = l0.wrap(
    AsyncOpenAI(),
    guardrails=l0.Guardrails.recommended(),
    retry=l0.Retry(attempts=5),
    timeout=l0.Timeout(initial_token=10000, inter_token=30000),  # Milliseconds
    continue_from_last_good_token=True,  # Resume from checkpoint on failure
    on_event=lambda e: print(f"[{e.type}]"),
    context={"request_id": "req-123", "user_id": "user-456"},
)
```

#### Wrapping a Raw Stream (Simple Cases)

```python
# For one-off streams without retry support
raw_stream = await client.chat.completions.create(..., stream=True)
result = l0.wrap(raw_stream)
text = await result.read()
```

**Parameters:**

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `client_or_stream` | `Client \| AsyncIterator` | required | OpenAI/LiteLLM client or raw stream |
| `guardrails` | `list[GuardrailRule]` | `None` | Guardrail rules to apply |
| `retry` | `Retry` | `Retry.recommended()` | Retry configuration (clients only) |
| `timeout` | `Timeout` | `None` | Timeout configuration |
| `continue_from_last_good_token` | `bool \| ContinuationConfig` | `False` | Resume from checkpoint on failure |
| `adapter` | `str \| Adapter` | `None` | Adapter hint or instance |
| `on_event` | `Callable[[ObservabilityEvent], None]` | `None` | Observability callback |
| `on_token` | `Callable[[str], None]` | `None` | Callback for each token received |
| `on_tool_call` | `Callable[[str, str, dict[str, Any]], None]` | `None` | Callback for tool calls (name, id, args) |
| `on_violation` | `Callable[[GuardrailViolation], None]` | `None` | Callback for guardrail violations |
| `context` | `dict` | `None` | User context attached to all events |
| `buffer_tool_calls` | `bool` | `False` | Buffer tool call arguments until complete |
| `build_continuation_prompt` | `Callable[[str], str]` | `None` | Modify prompt for continuation |

**Returns:** 
- `WrappedClient` when passed a client (has `.chat.completions.create()`)
- `LazyStream` when passed a raw stream

---

### run(stream, *, fallbacks, guardrails, drift_detector, retry, timeout, check_intervals, adapter, on_event, context, buffer_tool_calls, continue_from_last_good_token, callbacks, on_start, on_complete, on_error, on_stream_event, on_violation, on_retry, on_fallback, on_resume, on_checkpoint, on_timeout, on_abort, on_drift, on_tool_call)

Run L0 with a stream factory. Use when you need **retries or fallbacks** (which require re-creating the stream).

> **Note:** `l0()` is an alias to `run()` for convenience. Both work identically.

```python
import l0

result = await l0.run(
    # Required: Stream factory (lambda for retries)
    stream=lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        stream=True,
    ),

    # Optional: Fallback streams
    fallbacks=[
        lambda: client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        ),
    ],

    # Optional: Guardrails
    guardrails=l0.Guardrails.recommended(),

    # Optional: Retry configuration (defaults shown)
    retry=l0.Retry(
        attempts=3,                              # LLM errors only
        max_retries=6,                           # Total (LLM + network)
        base_delay=1.0,                          # Seconds
        max_delay=10.0,                          # Seconds
        strategy=l0.BackoffStrategy.FIXED_JITTER,
    ),

    # Optional: Timeout configuration (defaults shown, in milliseconds)
    timeout=l0.Timeout(
        initial_token=5000,   # Milliseconds to first token
        inter_token=10000,    # Milliseconds between tokens
    ),

    # Optional: Adapter hint
    adapter="openai",  # or "litellm", or Adapter instance

    # Optional: Event callback
    on_event=lambda event: print(f"[{event.type}]"),

    # Optional: User context attached to all events
    context={"request_id": "req-123", "tenant": "acme"},
    
    # Optional: Resume from checkpoint on failure
    continue_from_last_good_token=True,
)

# Iterate with Pythonic event properties
async for event in result:
    if event.is_token:
        print(event.text, end="")
    elif event.is_tool_call:
        print(f"Tool call: {event.data}")
    elif event.is_complete:
        print("\nComplete")
        print(f"Usage: {event.usage}")
    elif event.is_error:
        print(f"Error: {event.error}")

# Or get full text directly
text = await result.read()

# Access state anytime
print(result.state.content)       # Full accumulated content
print(result.state.token_count)   # Total tokens received
print(result.state.checkpoint)    # Last stable checkpoint
print(result.state.duration)      # Duration in seconds
```

**Parameters:**

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `stream` | `Callable[[], AsyncIterator]` | required | Factory returning async LLM stream |
| `fallbacks` | `list[Callable]` | `None` | Fallback stream factories |
| `guardrails` | `list[GuardrailRule]` | `None` | Guardrail rules to apply |
| `drift_detector` | `DriftDetector` | `None` | Drift detector for detecting model derailment |
| `retry` | `Retry` | `None` | Retry configuration |
| `timeout` | `Timeout` | `None` | Timeout configuration |
| `check_intervals` | `CheckIntervals` | `None` | Check intervals for guardrails/drift/checkpoint |
| `adapter` | `str \| Adapter` | `None` | Adapter hint or instance |
| `on_event` | `Callable[[ObservabilityEvent], None]` | `None` | Observability callback |
| `context` | `dict` | `None` | User context attached to all events |
| `buffer_tool_calls` | `bool` | `False` | Buffer tool call arguments until complete |
| `continue_from_last_good_token` | `bool \| ContinuationConfig` | `False` | Resume from checkpoint on failure |
| `build_continuation_prompt` | `Callable[[str], str]` | `None` | Modify prompt for continuation |
| `callbacks` | `LifecycleCallbacks` | `None` | Lifecycle callbacks object (alternative to individual params) |
| `on_start` | `Callable[[int, bool, bool], None]` | `None` | Called when execution attempt begins (attempt, is_retry, is_fallback) |
| `on_complete` | `Callable[[State], None]` | `None` | Called when stream completes (state) |
| `on_error` | `Callable[[Exception, bool, bool], None]` | `None` | Called when error occurs (error, will_retry, will_fallback) |
| `on_stream_event` | `Callable[[Event], None]` | `None` | Called for every L0 event (event) |
| `on_violation` | `Callable[[GuardrailViolation], None]` | `None` | Called when guardrail violation detected (violation) |
| `on_retry` | `Callable[[int, str], None]` | `None` | Called when retry triggered (attempt, reason) |
| `on_fallback` | `Callable[[int, str], None]` | `None` | Called when switching to fallback (index, reason) |
| `on_resume` | `Callable[[str, int], None]` | `None` | Called when resuming from checkpoint (checkpoint, token_count) |
| `on_checkpoint` | `Callable[[str, int], None]` | `None` | Called when checkpoint saved (checkpoint, token_count) |
| `on_timeout` | `Callable[[str, float], None]` | `None` | Called when timeout occurs (type, elapsed_seconds) |
| `on_abort` | `Callable[[int, int], None]` | `None` | Called when stream aborted (token_count, content_length) |
| `on_drift` | `Callable[[list[str], float \| None], None]` | `None` | Called when drift detected (drift_types, confidence) |
| `on_tool_call` | `Callable[[str, str, dict[str, Any]], None]` | `None` | Called when tool call detected (name, id, args) |

**Returns:** `Stream` - Async iterator with attached state

| Property/Method | Type | Description |
| --------------- | ---- | ----------- |
| `__aiter__` | - | Iterate directly over events |
| `state` | `State` | Runtime state |
| `abort()` | `Callable[[], None]` | Abort the stream |
| `read()` | `async -> str` | Consume stream, return full text |
| `errors` | `list[Exception]` | Errors encountered |

### wrap() vs run()

| Function | When to Use | Returns |
| -------- | ----------- | ------- |
| `wrap(client)` | **Recommended** - Wrap OpenAI client once, use everywhere | `WrappedClient` |
| `wrap(stream)` | Simple one-off, no retry support | `LazyStream` |
| `run()` | Need fallbacks or LiteLLM | `Stream` |

```python
# Recommended - wrap client
client = l0.wrap(AsyncOpenAI())
response = await client.chat.completions.create(...)

# Simple one-off stream
result = l0.wrap(raw_stream)
text = await result.read()

# With fallbacks - use run()
result = await l0.run(
    stream=lambda: create_stream(),
    fallbacks=[lambda: backup_stream()],
)
```

### WrappedClient

When you wrap an OpenAI client, you get a `WrappedClient` that mirrors the original API:

```python
client = l0.wrap(AsyncOpenAI())

# Same API as OpenAI
response = await client.chat.completions.create(
    model="gpt-4o",
    messages=[...],
    stream=True,
)

# Access the underlying client
raw_client = client.unwrapped

# Create a new client with different options
strict_client = client.with_options(
    guardrails=l0.Guardrails.strict(),
    continue_from_last_good_token=True,
)
```

---

## Lifecycle Callbacks

L0 provides lifecycle callbacks for monitoring and responding to runtime events. All callbacks are optional and are pure side-effect handlers (they don't affect execution flow).

### Callback Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            L0 LIFECYCLE FLOW                                │
└─────────────────────────────────────────────────────────────────────────────┘

                                ┌──────────┐
                                │  START   │
                                └────┬─────┘
                                     │
                                     ▼
                      ┌──────────────────────────────┐
                      │       on_event(event)        │
                      └──────────────┬───────────────┘
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              STREAMING PHASE                               │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         on_event(event)                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  During streaming, events fire as conditions occur:                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  CHECKPOINT  │  │  TOOL_CALL   │  │    DRIFT     │  │   TIMEOUT    │   │
│  │    SAVED     │  │   detected   │  │   detected   │  │   occurred   │   │
│  └──────────────┘  └──────────────┘  └──────┬───────┘  └──────┬───────┘   │
│                                             │                  │           │
│                                             └────────┬─────────┘           │
│                                                      │ triggers retry      │
└──────────────────────────────────────────────────────┼─────────────────────┘
                                                       │
              ┌────────────────────────────────────────┼────────────────┐
              │                    │                   │                │
              ▼                    ▼                   ▼                ▼
        ┌─────────┐          ┌───────────┐      ┌──────────┐      ┌─────────┐
        │ SUCCESS │          │   ERROR   │      │VIOLATION │      │  ABORT  │
        └────┬────┘          └─────┬─────┘      └────┬─────┘      └────┬────┘
             │                     │                 │                 │
             │                     ▼                 ▼                 ▼
             │              ┌────────────────────────────────┐   ┌───────────┐
             │              │      on_event(ERROR)           │   │ ABORTED   │
             │              └──────────────┬─────────────────┘   └───────────┘
             │                             │
             │                 ┌───────────┼───────────┐
             │                 │           │           │
             │                 ▼           ▼           ▼
             │           ┌──────────┐ ┌──────────┐ ┌──────────┐
             │           │  RETRY   │ │ FALLBACK │ │  FATAL   │
             │           └────┬─────┘ └────┬─────┘ └────┬─────┘
             │                │            │            │
             │                │    ┌───────┘            │
             │                │    │                    │
             │                ▼    ▼                    │
             │          ┌─────────────────────┐         │
             │          │  Has checkpoint?    │         │
             │          └──────────┬──────────┘         │
             │                YES  │  NO                │
             │                ┌────┴────┐               │
             │                ▼         ▼               │
             │          ┌──────────┐    │               │
             │          │  RESUME  │    │               │
             │          └────┬─────┘    │               │
             │               │          │               │
             │               ▼          ▼               │
             │          ┌─────────────────────────┐     │
             │          │    Back to STREAMING    │─────┘
             │          └─────────────────────────┘
             │
             ▼
      ┌─────────────┐
      │  COMPLETE   │
      └─────────────┘
```

### Callback Reference

| Callback | Signature | When Called |
| -------- | --------- | ----------- |
| `on_event` | `(event: ObservabilityEvent) -> None` | Any runtime event emitted |

### ObservabilityEventType Reference

| Event Type | Description |
| ---------- | ----------- |
| **Session** | |
| `SESSION_START` | New execution session begins |
| `ATTEMPT_START` | New attempt within session begins |
| `SESSION_END` | Session completed |
| `SESSION_SUMMARY` | Session summary with aggregated stats |
| **Stream** | |
| `STREAM_INIT` | Stream initialized |
| `STREAM_READY` | Stream ready for tokens |
| `TOKEN` | Token received |
| **Adapter** | |
| `ADAPTER_DETECTED` | Adapter auto-detected for stream |
| `ADAPTER_WRAP_START` | Adapter wrap starting |
| `ADAPTER_WRAP_END` | Adapter wrap completed |
| **Timeout** | |
| `TIMEOUT_START` | Timeout timer started |
| `TIMEOUT_RESET` | Timeout timer reset (token received) |
| `TIMEOUT_TRIGGERED` | Timeout threshold exceeded |
| **Network** | |
| `NETWORK_ERROR` | Network error occurred |
| `NETWORK_RECOVERY` | Recovered from network error |
| `CONNECTION_DROPPED` | Connection dropped mid-stream |
| `CONNECTION_RESTORED` | Connection restored after drop |
| **Abort** | |
| `ABORT_REQUESTED` | Abort requested by user |
| `ABORT_COMPLETED` | Abort completed |
| **Tool** | |
| `TOOL_REQUESTED` | Tool call requested |
| `TOOL_START` | Tool execution starting |
| `TOOL_RESULT` | Tool returned result |
| `TOOL_ERROR` | Tool execution failed |
| `TOOL_COMPLETED` | Tool call completed |
| **Guardrail** | |
| `GUARDRAIL_PHASE_START` | Guardrail check phase starting |
| `GUARDRAIL_PHASE_END` | Guardrail check phase completed |
| `GUARDRAIL_RULE_START` | Individual rule check starting |
| `GUARDRAIL_RULE_RESULT` | Individual rule result |
| `GUARDRAIL_RULE_END` | Individual rule check completed |
| `GUARDRAIL_CALLBACK_START` | Guardrail callback starting |
| `GUARDRAIL_CALLBACK_END` | Guardrail callback completed |
| **Drift** | |
| `DRIFT_CHECK_RESULT` | Drift detection result |
| `DRIFT_CHECK_SKIPPED` | Drift check skipped |
| **Checkpoint** | |
| `CHECKPOINT_SAVED` | Checkpoint saved |
| **Resume** | |
| `RESUME_START` | Resuming from checkpoint |
| **Retry** | |
| `RETRY_START` | Retry sequence starting |
| `RETRY_ATTEMPT` | Individual retry attempt |
| `RETRY_END` | Retry sequence completed |
| `RETRY_GIVE_UP` | All retries exhausted |
| `RETRY_FN_START` | Custom retry function starting |
| `RETRY_FN_RESULT` | Custom retry function result |
| `RETRY_FN_ERROR` | Custom retry function error |
| **Fallback** | |
| `FALLBACK_START` | Switching to fallback model |
| `FALLBACK_MODEL_SELECTED` | Fallback model selected |
| `FALLBACK_END` | Fallback sequence completed |
| **Completion** | |
| `FINALIZATION_START` | Finalization phase starting |
| `FINALIZATION_END` | Finalization phase completed |
| `COMPLETE` | Stream completed successfully |
| `ERROR` | Error occurred |
| **Consensus** | |
| `CONSENSUS_START` | Consensus operation starting |
| `CONSENSUS_STREAM_START` | Individual consensus stream starting |
| `CONSENSUS_STREAM_END` | Individual consensus stream completed |
| `CONSENSUS_OUTPUT_COLLECTED` | Output collected for consensus |
| `CONSENSUS_ANALYSIS` | Consensus analysis performed |
| `CONSENSUS_RESOLUTION` | Consensus resolved |
| `CONSENSUS_END` | Consensus operation completed |
| **Structured Output** | |
| `PARSE_START` | JSON parsing starting |
| `PARSE_END` | JSON parsing completed |
| `PARSE_ERROR` | JSON parsing error |
| `SCHEMA_VALIDATION_START` | Schema validation starting |
| `SCHEMA_VALIDATION_END` | Schema validation completed |
| `SCHEMA_VALIDATION_ERROR` | Schema validation error |
| `AUTO_CORRECT_START` | JSON auto-correction starting |
| `AUTO_CORRECT_END` | JSON auto-correction completed |
| **Continuation** | |
| `CONTINUATION_START` | Continuation starting |

### Usage Example

```python
import l0

def handle_event(event: l0.ObservabilityEvent):
    match event.type:
        case l0.ObservabilityEventType.SESSION_START:
            print(f"Session started: {event.stream_id}")
        case l0.ObservabilityEventType.RETRY_ATTEMPT:
            print(f"Retrying (attempt {event.meta.get('attempt', '?')})")
        case l0.ObservabilityEventType.FALLBACK_START:
            print(f"Switching to fallback {event.meta.get('toIndex', '?')}")
        case l0.ObservabilityEventType.CHECKPOINT_SAVED:
            print(f"Checkpoint saved ({event.meta.get('token_count', 0)} tokens)")
        case l0.ObservabilityEventType.NETWORK_ERROR:
            print(f"Network error: {event.meta.get('error', 'unknown')}")
        case l0.ObservabilityEventType.COMPLETE:
            print(f"Complete! Duration: {event.meta.get('duration', 0)}s")
        case l0.ObservabilityEventType.ERROR:
            print(f"Error: {event.meta.get('error', 'unknown')}")

result = await l0.run(
    stream=lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        stream=True,
    ),
    on_event=handle_event,
    context={"request_id": "req-123", "user_id": "user-456"},
)
```

---

## Streaming Runtime

L0 wraps LLM streams with deterministic behavior and unified event types.

### Unified Event Format

All streams are normalized to `Event` objects:

```python
@dataclass
class Event:
    type: EventType                           # Event type
    text: str | None = None                   # Token content
    data: dict[str, Any] | None = None        # Tool call / misc data
    payload: DataPayload | None = None        # Multimodal data payload
    progress: Progress | None = None          # Progress update
    error: Exception | None = None            # Error (for error events)
    usage: dict[str, int] | None = None       # Token usage
    timestamp: float | None = None            # Event timestamp

    # Pythonic type check properties
    @property
    def is_token(self) -> bool: ...
    @property
    def is_message(self) -> bool: ...
    @property
    def is_data(self) -> bool: ...
    @property
    def is_progress(self) -> bool: ...
    @property
    def is_tool_call(self) -> bool: ...
    @property
    def is_error(self) -> bool: ...
    @property
    def is_complete(self) -> bool: ...
```

### Event Types

```python
class EventType(str, Enum):
    TOKEN = "token"           # Text token
    MESSAGE = "message"       # Full message
    DATA = "data"             # Structured data
    PROGRESS = "progress"     # Progress update
    TOOL_CALL = "tool_call"   # Tool/function call
    ERROR = "error"           # Error occurred
    COMPLETE = "complete"     # Stream complete
```

### Tool Call Handling

```python
result = await l0.run(
    stream=lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": "What's the weather?"}],
        tools=[{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get weather for a location",
                "parameters": {
                    "type": "object",
                    "properties": {"location": {"type": "string"}},
                    "required": ["location"],
                },
            },
        }],
        stream=True,
    ),
)

async for event in result:
    if event.is_tool_call:
        print(f"Tool: {event.data['name']}")
        print(f"Args: {event.data['arguments']}")
        print(f"ID: {event.data['id']}")
```

### State Tracking

```python
# Access state at any point
state = result.state

state.content           # Accumulated content
state.checkpoint        # Last validated checkpoint
state.token_count       # Total tokens received
state.model_retry_count # Model error retries
state.network_retry_count # Network error retries
state.fallback_index    # Current model (0=primary)
state.violations        # Guardrail violations
state.drift_detected    # Whether drift was detected
state.completed         # Stream completed successfully
state.aborted           # Stream was aborted
state.first_token_at    # Timestamp of first token
state.last_token_at     # Timestamp of last token
state.duration          # Total duration (seconds)
state.resumed           # Resumed from checkpoint
state.data_outputs      # List of DataPayload from multimodal outputs
state.last_progress     # Last Progress update received
```

---

## Retry Configuration

### Retry

All delays are in **seconds** (float), matching Python conventions like `asyncio.sleep()`.

```python
@dataclass
class Retry:
    attempts: int | None = None                   # Model errors only
    max_retries: int | None = None                # Absolute cap (all errors)
    base_delay: float | None = None               # Starting delay (seconds)
    max_delay: float | None = None                # Maximum delay (seconds)
    strategy: BackoffStrategy | None = None       # Backoff strategy
    error_type_delays: ErrorTypeDelays | None = None  # Per-error-type delays
    retry_on: list[RetryableErrorType] | None = None  # Which error types to retry
    should_retry: Callable[..., bool | Coroutine] | None = None  # Veto callback
    calculate_delay: Callable[..., float] | None = None  # Custom delay calculation
```

> **Note:** All fields default to `None` and are populated via `__post_init__` from `RETRY_DEFAULTS`. Effective defaults: `attempts=3`, `max_retries=6`, `base_delay=1.0`, `max_delay=10.0`, `strategy=BackoffStrategy.FIXED_JITTER`.

### BackoffStrategy

```python
class BackoffStrategy(str, Enum):
    EXPONENTIAL = "exponential"    # delay * 2^attempt
    LINEAR = "linear"              # delay * (attempt + 1)
    FIXED = "fixed"                # constant delay
    FULL_JITTER = "full-jitter"    # random(0, exponential)
    FIXED_JITTER = "fixed-jitter"  # base/2 + random(base/2) - DEFAULT
```

### Backoff Calculation

| Strategy | Formula | Example (base=1.0s, attempt=2) |
| -------- | ------- | -------------------------------- |
| `EXPONENTIAL` | `min(base * 2^attempt, max)` | 4.0s |
| `LINEAR` | `min(base * (attempt + 1), max)` | 3.0s |
| `FIXED` | `base` | 1.0s |
| `FULL_JITTER` | `random(0, min(base * 2^attempt, max))` | 0-4.0s |
| `FIXED_JITTER` | `temp/2 + random(temp/2)` | 2.0-4.0s |

### Retry Behavior by Error Type

| Error Type | Retries | Counts Toward `attempts` | Counts Toward `max_retries` |
| ---------- | ------- | ------------------------ | --------------------------- |
| Network disconnect | Yes | No | Yes |
| Zero output | Yes | No | Yes |
| Timeout | Yes | No | Yes |
| 429 rate limit | Yes | No | Yes |
| 503 server error | Yes | No | Yes |
| Guardrail violation | Yes | **Yes** | Yes |
| Drift detected | Yes | **Yes** | Yes |
| Auth error (401/403) | No | - | - |

### Retry Presets

```python
from l0 import (
    MINIMAL_RETRY,      # 2 attempts, 4 max, linear backoff
    RECOMMENDED_RETRY,  # 3 attempts, 6 max, fixed-jitter backoff
    STRICT_RETRY,       # 3 attempts, 6 max, full-jitter backoff
    EXPONENTIAL_RETRY,  # 4 attempts, 8 max, exponential backoff
    Retry,
)

# Use preset directly
result = await l0.run(stream=my_stream, retry=RECOMMENDED_RETRY)

# Or use class method presets
result = await l0.run(stream=my_stream, retry=Retry.recommended())
result = await l0.run(stream=my_stream, retry=Retry.minimal())
result = await l0.run(stream=my_stream, retry=Retry.strict())
result = await l0.run(stream=my_stream, retry=Retry.exponential())

# Environment-specific presets
result = await l0.run(stream=my_stream, retry=Retry.mobile())  # Higher delays for mobile
result = await l0.run(stream=my_stream, retry=Retry.edge())    # Shorter delays for edge runtimes
```

| Preset | attempts | max_retries | backoff | base_delay | max_delay |
| ------ | -------- | ----------- | ------- | ---------- | --------- |
| `MINIMAL_RETRY` | 2 | 4 | `linear` | 1.0s | 10.0s |
| `RECOMMENDED_RETRY` | 3 | 6 | `fixed-jitter` | 1.0s | 10.0s |
| `STRICT_RETRY` | 3 | 6 | `full-jitter` | 1.0s | 10.0s |
| `EXPONENTIAL_RETRY` | 4 | 8 | `exponential` | 1.0s | 10.0s |

### Centralized Defaults

```python
from l0 import RETRY_DEFAULTS, ERROR_TYPE_DELAY_DEFAULTS

# RETRY_DEFAULTS contains all default values
RETRY_DEFAULTS.attempts      # 3
RETRY_DEFAULTS.max_retries   # 6
RETRY_DEFAULTS.base_delay    # 1.0 (seconds)
RETRY_DEFAULTS.max_delay     # 10.0 (seconds)
RETRY_DEFAULTS.backoff       # BackoffStrategy.FIXED_JITTER

# ERROR_TYPE_DELAY_DEFAULTS for network error types
ERROR_TYPE_DELAY_DEFAULTS.connection_dropped  # 1.0s
ERROR_TYPE_DELAY_DEFAULTS.timeout             # 1.0s
ERROR_TYPE_DELAY_DEFAULTS.dns_error           # 3.0s
ERROR_TYPE_DELAY_DEFAULTS.ssl_error           # 0.0s (don't retry SSL errors)
```

### Custom Retry Logic

Override default retry behavior with custom functions.

#### shouldRetry (Async Veto Callback)

The `should_retry` callback provides async control over retry decisions. It can only **veto** retries, never force them.

```python
from l0 import Retry, State, ErrorCategory

async def custom_should_retry(
    error: Exception,
    state: State,
    attempt: int,
    category: ErrorCategory
) -> bool:
    # Veto retry if we already have substantial content
    if state.token_count > 100:
        return False
    
    # Veto retry for context length errors
    if "context_length_exceeded" in str(error):
        return False
    
    # Check external service before retrying
    can_retry = await check_rate_limit_service()
    if not can_retry:
        return False
    
    # Return True to allow default retry behavior
    return True

result = await l0.run(
    stream=my_stream,
    retry=Retry(
        attempts=5,
        should_retry=custom_should_retry,
    ),
)
```

#### Key Behavior

The final retry decision follows this formula:

```
final_should_retry = default_decision AND should_retry(...)
```

| Default Decision | should_retry Returns | Final Result | Explanation |
| ---------------- | -------------------- | ------------ | ----------- |
| `True` | `True` | **Retry** | Both agree to retry |
| `True` | `False` | **No retry** | User vetoed the retry |
| `False` | `True` | **No retry** | User cannot force retry |
| `False` | `False` | **No retry** | Both agree not to retry |

#### should_retry Parameters

| Parameter | Type | Description |
| --------- | ---- | ----------- |
| `error` | `Exception` | The error that occurred |
| `state` | `State` | Current state (content, token_count, etc.) |
| `attempt` | `int` | Current attempt (0-based) |
| `category` | `ErrorCategory` | Error category (network/transient/model/fatal) |

#### calculateDelay

Custom delay calculation function to override default backoff behavior:

```python
from l0 import Retry

def custom_calculate_delay(context: dict) -> float:
    """
    context contains:
    - attempt: int - Current retry attempt (0-based)
    - total_attempts: int - Total attempts including network
    - category: str - Error category (network/model/fatal)
    - reason: str - Error reason code
    - error: Exception - The error that occurred
    - default_delay: float - Default delay that would be used
    """
    # Different delays based on error category
    if context["category"] == "network":
        return 0.5
    if context["reason"] == "rate_limit":
        return 5.0
    
    # Custom exponential backoff with full jitter
    import random
    base = 1.0
    cap = 30.0
    temp = min(cap, base * (2 ** context["attempt"]))
    return random.random() * temp

result = await l0.run(
    stream=my_stream,
    retry=Retry(
        attempts=3,
        base_delay=1.0,
        calculate_delay=custom_calculate_delay,
    ),
)
```

### Error Type Delays

Custom delays for specific network error types. Overrides `base_delay` for fine-grained control.

```python
from l0 import Retry, ErrorTypeDelays

result = await l0.run(
    stream=my_stream,
    retry=Retry(
        attempts=3,
        error_type_delays=ErrorTypeDelays(
            # Connection errors
            connection_dropped=2.0,   # Connection dropped mid-stream
            econnreset=1.5,           # Connection reset by peer
            econnrefused=3.0,         # Connection refused
            
            # Network errors
            fetch_error=0.5,          # Generic fetch failure
            dns_error=5.0,            # DNS resolution failed
            timeout=1.5,              # Request timeout
            
            # Streaming errors
            sse_aborted=1.0,          # Server-sent events aborted
            no_bytes=0.5,             # No bytes received
            partial_chunks=1.0,       # Incomplete chunks received
            
            # Runtime errors
            runtime_killed=5.0,       # Runtime process killed
            background_throttle=2.0,  # Background tab throttling
            
            # Fallback
            unknown=1.0,              # Unknown error type
        ),
    ),
)
```

### Retryable Error Types

```python
from l0 import Retry, RetryableErrorType

# Only retry on specific error types
result = await l0.run(
    stream=my_stream,
    retry=Retry(
        attempts=3,
        retry_on=[
            RetryableErrorType.NETWORK_ERROR,
            RetryableErrorType.TIMEOUT,
            RetryableErrorType.RATE_LIMIT,
            # Exclude: ZERO_OUTPUT, GUARDRAIL_VIOLATION, DRIFT, etc.
        ],
    ),
)
```

Available error types:

| Error Type | Description |
| ---------- | ----------- |
| `ZERO_OUTPUT` | No meaningful output generated |
| `GUARDRAIL_VIOLATION` | Guardrail rule failed |
| `DRIFT` | Output drift detected |
| `INCOMPLETE` | Incomplete output |
| `NETWORK_ERROR` | Network/connection error |
| `TIMEOUT` | Request timeout |
| `RATE_LIMIT` | 429 rate limit |
| `SERVER_ERROR` | 5xx server error |

---

## Checkpoint Resumption

When a stream fails mid-generation (timeout, network error), L0 can resume from the last checkpoint instead of starting over.

### continue_from_last_good_token

Enable with `continue_from_last_good_token=True`:

```python
client = l0.wrap(
    AsyncOpenAI(),
    continue_from_last_good_token=True,
    timeout=l0.Timeout(inter_token=30000),
)

# If the stream times out after "Hello wor", L0 will:
# 1. Save checkpoint: "Hello wor"
# 2. Retry the request
# 3. Deduplicate any overlapping content from the retry
# 4. Continue seamlessly
```

### How It Works

1. **Checkpoint Saving**: L0 saves checkpoints at configurable intervals (default: every 5 tokens)
2. **Failure Detection**: On timeout or transient error, the checkpoint is preserved
3. **Retry with Continuation**: On retry, the checkpoint content is available
4. **Deduplication**: If the LLM repeats content from the checkpoint, L0 removes the overlap

### ContinuationConfig

For fine-grained control:

```python
from l0 import ContinuationConfig, DeduplicationOptions

config = ContinuationConfig(
    enabled=True,
    checkpoint_interval=5,        # Save checkpoint every N tokens
    deduplicate=True,             # Remove overlapping content
    deduplication_options=DeduplicationOptions(
        min_overlap=2,            # Minimum chars to consider overlap
        max_overlap=500,          # Maximum chars to check
        case_sensitive=True,
        normalize_whitespace=False,
    ),
    validate_checkpoint=True,     # Run guardrails on checkpoint
)

client = l0.wrap(
    AsyncOpenAI(),
    continue_from_last_good_token=config,
)
```

### State Fields

After completion, check continuation state:

```python
response = await client.chat.completions.create(...)
async for event in response:
    pass

# Check if continuation was used
print(response.state.resumed)              # True if retried
print(response.state.checkpoint)           # Last checkpoint content
print(response.state.continuation_used)    # True if resumed from checkpoint
print(response.state.deduplication_applied)  # True if overlap removed
print(response.state.overlap_removed)      # The overlapping text that was removed
```

---

## Smart Continuation Deduplication

When using `continue_from_last_good_token`, LLMs often repeat words from the end of the checkpoint at the beginning of their continuation. L0 automatically detects and removes this overlap.

### How It Works

```python
# Checkpoint: "Hello world"
# LLM continues with: "world is great"
# Without deduplication: "Hello worldworld is great"
# With deduplication: "Hello world is great" ✓
```

Deduplication is **enabled by default** when `continue_from_last_good_token=True`. The algorithm:

1. Buffers incoming continuation tokens until overlap can be detected
2. Finds the longest suffix of the checkpoint that matches a prefix of the continuation
3. Removes the overlapping portion from the continuation
4. Emits only the non-overlapping content

### Configuration

```python
from l0 import ContinuationConfig, DeduplicationOptions

config = ContinuationConfig(
    enabled=True,
    checkpoint_interval=5,
    
    # Deduplication enabled by default, explicitly disable:
    deduplicate=False,
    
    # Or configure options:
    deduplication_options=DeduplicationOptions(
        min_overlap=2,       # Minimum chars to consider overlap (default: 2)
        max_overlap=500,     # Maximum chars to check (default: 500)
        case_sensitive=True, # Case-sensitive matching (default: True)
        normalize_whitespace=False,  # Normalize whitespace for matching (default: False)
    ),
)

result = await l0.run(
    stream=lambda: client.chat.completions.create(..., stream=True),
    continue_from_last_good_token=config,
)
```

### Options

| Option                | Type    | Default | Description                                                                   |
| --------------------- | ------- | ------- | ----------------------------------------------------------------------------- |
| `min_overlap`         | int     | 2       | Minimum overlap length to detect (avoids false positives)                     |
| `max_overlap`         | int     | 500     | Maximum overlap length to check (performance limit)                           |
| `case_sensitive`      | bool    | True    | Whether matching is case-sensitive                                            |
| `normalize_whitespace`| bool    | False   | Normalize whitespace when matching (`"hello  world"` matches `"hello world"`) |

### Utility Functions

The overlap detection is also available as standalone utilities:

```python
from l0 import Continuation

# Full result with metadata
result = Continuation.detect_overlap("Hello world", "world is great")
# OverlapResult(
#     has_overlap=True,
#     overlap_length=5,
#     overlap_text="world",
#     deduplicated=" is great"
# )

# Convenience wrapper - just the deduplicated string
text = Continuation.deduplicate("Hello world", "world is great")
# " is great"

# With options
from l0 import DeduplicationOptions

options = DeduplicationOptions(case_sensitive=False, min_overlap=3)
result = Continuation.detect_overlap("Hello World", "world test", options)
```

### Examples

**Case-insensitive matching:**

```python
# Checkpoint: "Hello World"
# Continuation: "world is great"
# With case_sensitive=False → "Hello World is great"

config = ContinuationConfig(
    enabled=True,
    deduplication_options=DeduplicationOptions(case_sensitive=False),
)
```

**Multi-word overlap:**

```python
# Checkpoint: "The quick brown fox"
# Continuation: "brown fox jumps over"
# Result: "The quick brown fox jumps over"
```

---

## Document Windows

Process documents that exceed context limits with automatic chunking.

### Window.create(document, *, size, overlap, strategy)

Create a window for processing long documents.

```python
from l0 import Window

window = Window.create(
    long_document,
    size=2000,           # Tokens per chunk
    overlap=200,         # Overlap between chunks
    strategy="paragraph", # "token" | "char" | "paragraph" | "sentence"
)

# Navigation
chunk = window.current()     # Current chunk
window.next()                # Move to next
window.prev()                # Move to previous
window.jump(5)               # Jump to chunk 5

# Search
matches = window.find_chunks("keyword")

# Get context around a chunk
context = window.get_context(chunk_index, before=1, after=1)

# Statistics
stats = window.get_stats()
print(f"Total chunks: {stats.total_chunks}")
print(f"Total tokens: {stats.total_tokens}")
```

### Window Presets

```python
from l0 import Window

# Quick creation with presets
window = Window.small(document)      # 1000 tokens, 100 overlap
window = Window.medium(document)     # 2000 tokens, 200 overlap (default)
window = Window.large(document)      # 4000 tokens, 400 overlap
window = Window.paragraph(document)  # Paragraph-based chunking
window = Window.sentence(document)   # Sentence-based chunking
```

### Processing All Chunks

```python
from l0 import Window, ChunkProcessConfig

window = Window.create(document, size=2000, overlap=200)

# Process all chunks in parallel
results = await window.process_all(
    lambda chunk: ChunkProcessConfig(
        stream=lambda: client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"Summarize: {chunk.content}"}],
            stream=True,
        ),
    ),
    concurrency=3,  # Max 3 concurrent
)

# Check results
for result in results:
    if result.status == "success":
        print(f"Chunk {result.chunk.index}: {result.content[:50]}...")
    else:
        print(f"Chunk {result.chunk.index} failed: {result.error}")

# Get processing statistics
stats = Window.get_stats(results)
print(f"Success rate: {stats.success_rate}%")
print(f"Average duration: {stats.avg_duration}ms")
```

### Merging Results

```python
# Merge all successful results
merged_text = Window.merge_results(results, separator="\n\n")

# Merge chunks back into document (handles overlap)
merged_doc = Window.merge_chunks(window.get_all_chunks())
```

### DocumentChunk

```python
@dataclass
class DocumentChunk:
    index: int          # Position (0-based)
    content: str        # Chunk text
    start_pos: int      # Start position in original document
    end_pos: int        # End position in original document
    token_count: int    # Estimated tokens
    char_count: int     # Character count
    is_first: bool      # Is this the first chunk?
    is_last: bool       # Is this the last chunk?
    total_chunks: int   # Total number of chunks
    metadata: dict      # Custom metadata
```

---

## Pipeline

Multi-phase streaming workflows where each step receives the output of the previous step.

### pipe(steps, input, options)

Execute a pipeline of streaming steps.

```python
from openai import AsyncOpenAI
import l0

client = AsyncOpenAI()

async def summarize_step(text: str, ctx: l0.StepContext):
    return lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": f"Summarize: {text}"}],
        stream=True,
    )

async def refine_step(summary: str, ctx: l0.StepContext):
    return lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": f"Refine this summary: {summary}"}],
        stream=True,
    )

result = await l0.pipe(
    [
        l0.PipelineStep(name="summarize", fn=summarize_step),
        l0.PipelineStep(name="refine", fn=refine_step),
    ],
    long_document,
    l0.PipelineOptions(name="summarize-refine"),
)

print(result.output)  # Final refined summary
print(f"Duration: {result.duration}ms")
print(f"Steps completed: {len(result.steps)}")
```

### PipelineStep

```python
@dataclass
class PipelineStep:
    name: str           # Step name (for logging/debugging)
    fn: Callable        # Step function: (input, context) -> stream factory
    transform: Callable | None = None  # Transform output before next step
    condition: Callable | None = None  # Condition to run this step
    on_error: Callable | None = None   # Error handler for this step
    on_complete: Callable | None = None  # Callback when step completes
    metadata: dict = {}  # Step-specific metadata
```

### PipelineOptions

```python
@dataclass
class PipelineOptions:
    name: str | None = None      # Pipeline name
    stop_on_error: bool = True   # Stop on first error
    timeout: float | None = None # Max execution time (seconds)
    on_start: Callable | None = None     # Called when pipeline starts
    on_complete: Callable | None = None  # Called when pipeline completes
    on_error: Callable | None = None     # Called on error (error, step_index)
    on_progress: Callable | None = None  # Called for progress (step_index, total)
    metadata: dict = {}          # Pipeline-wide metadata
```

### Reusable Pipelines

```python
from l0 import create_pipeline, PipelineStep, PipelineOptions

# Create a reusable pipeline
summarize_pipeline = create_pipeline(
    [
        PipelineStep(name="extract", fn=extract_step),
        PipelineStep(name="summarize", fn=summarize_step),
        PipelineStep(name="format", fn=format_step),
    ],
    PipelineOptions(name="document-summarizer"),
)

# Run multiple times
result1 = await summarize_pipeline.run(document1)
result2 = await summarize_pipeline.run(document2)

# Clone and modify
strict_pipeline = summarize_pipeline.clone()
strict_pipeline.options.stop_on_error = True
```

### Conditional Steps

```python
from l0 import PipelineStep

# Step with condition
conditional_step = PipelineStep(
    name="translate",
    fn=translate_step,
    condition=lambda input, ctx: ctx.metadata.get("language") != "en",
)

# Branch step
from l0 import create_branch_step

branch = create_branch_step(
    "route",
    condition=lambda input, ctx: len(input) > 1000,
    if_true=summarize_step,   # Long text → summarize
    if_false=passthrough_step, # Short text → pass through
)
```

### Chaining and Parallel Pipelines

```python
from l0 import chain_pipelines, parallel_pipelines

# Chain pipelines sequentially
full_pipeline = chain_pipelines(
    extract_pipeline,
    analyze_pipeline,
    format_pipeline,
)

# Run pipelines in parallel and combine
results = await parallel_pipelines(
    [sentiment_pipeline, entity_pipeline, summary_pipeline],
    document,
    lambda results: {
        "sentiment": results[0].output,
        "entities": results[1].output,
        "summary": results[2].output,
    },
)
```

### Pipeline Presets

```python
from l0.pipeline import FAST_PIPELINE, RELIABLE_PIPELINE, PRODUCTION_PIPELINE

# FAST_PIPELINE: stop_on_error=True (fail fast)
# RELIABLE_PIPELINE: stop_on_error=False (graceful failures)
# PRODUCTION_PIPELINE: stop_on_error=False, timeout=300s
```

---

## Formatting Helpers

Utilities for formatting prompts, context, memory, and tool definitions.

### Context Formatting

```python
from l0 import Format

# Wrap content with delimiters
context = Format.context(
    "User manual content here",
    label="documentation",
    delimiter="xml",  # "xml" | "markdown" | "brackets" | "none"
)
# Output: <documentation>\nUser manual content here\n</documentation>

# Format multiple contexts
contexts = Format.contexts([
    {"content": "Doc 1", "label": "doc1"},
    {"content": "Doc 2", "label": "doc2"},
])

# Format a document with metadata
doc = Format.document(content, {"title": "Report", "author": "User"})

# Format instructions
instructions = Format.instructions("You are a helpful assistant")
```

### Memory Formatting

```python
from l0 import Format

# Format conversation history
memory = [
    {"role": "user", "content": "Hello"},
    {"role": "assistant", "content": "Hi there!"},
    {"role": "user", "content": "How are you?"},
]

formatted = Format.memory(memory, {"style": "conversational", "max_entries": 10})
# Output:
# User: Hello
# Assistant: Hi there!
# User: How are you?

# Create timestamped memory entries
entry = Format.memory_entry("user", "New message")

# Memory utilities
filtered = Format.filter_memory(memory, "user")  # Only user messages
last_5 = Format.last_n_entries(memory, 5)
size = Format.memory_size(memory)  # Character count
truncated = Format.truncate_memory(memory, max_size=1000)
```

### Output Formatting

```python
from l0 import Format

# Request JSON output
instruction = Format.json_output({"strict": True, "schema": "..."})

# Request structured output
instruction = Format.structured_output("yaml", {"strict": True})

# Define output constraints
constraints = Format.output_constraints({
    "max_length": 500,
    "format": "bullet_points",
})

# Clean model output
cleaned = Format.clean_output("Sure! Here's the JSON: {...}")  # "{...}"

# Extract JSON from output
json_str = Format.extract_json(model_output)

# Validate JSON
is_valid, error = Format.validate_json(output)
```

### Tool Formatting

```python
from l0 import Format

# Create a tool definition
tool = Format.create_tool(
    "search",
    "Search the web for information",
    [
        Format.parameter("query", "string", "Search query", required=True),
        Format.parameter("limit", "integer", "Max results", default=10),
    ],
)

# Format for model
formatted = Format.tool(tool, {"style": "json-schema"})

# Format multiple tools
formatted_tools = Format.tools([tool1, tool2])

# Parse function call from output
fn_call = Format.parse_function_call(model_output)
if fn_call:
    print(f"Function: {fn_call.name}, Args: {fn_call.arguments}")
```

### String Utilities

```python
from l0 import Format

# Basic operations
Format.trim("  hello  ")           # "hello"
Format.truncate("Hello World", 8)  # "Hello..."
Format.truncate_words("Hello World", 8)  # "Hello..."
Format.wrap("Long text...", 80)    # Word-wrapped text
Format.pad("hello", 10, align="center")  # "  hello   "

# Escaping
Format.escape("Hello\nWorld")      # "Hello\\nWorld"
Format.unescape("Hello\\nWorld")   # "Hello\nWorld"
Format.escape_html("<div>")        # "&lt;div&gt;"
Format.unescape_html("&lt;div&gt;") # "<div>"
Format.escape_regex("foo.*bar")    # "foo\\.\\*bar"
Format.sanitize("text\x00here")    # "texthere" (removes control chars)
Format.remove_ansi("\x1b[31mred\x1b[0m")  # "red"
```

---

## Network Protection

### Error Categorization

```python
from l0.errors import categorize_error
from l0.types import ErrorCategory

category = categorize_error(error)

match category:
    case ErrorCategory.NETWORK:
        print("Network error - retry forever")
    case ErrorCategory.TRANSIENT:
        print("Transient (429/503) - retry forever")
    case ErrorCategory.MODEL:
        print("Model error - counts toward limit")
    case ErrorCategory.CONTENT:
        print("Content error - counts toward limit")
    case ErrorCategory.PROVIDER:
        print("Provider error - may retry")
    case ErrorCategory.FATAL:
        print("Fatal - no retry (401/403)")
    case ErrorCategory.INTERNAL:
        print("Internal - no retry (bug)")
```

### Network Error Patterns

L0 automatically detects these patterns in error messages:

| Pattern | Description |
| ------- | ----------- |
| `connection.*reset` | Connection reset by peer |
| `connection.*refused` | Connection refused |
| `connection.*timeout` | Connection timeout |
| `timed?\s*out` | Request timed out |
| `dns.*failed` | DNS resolution failed |
| `name.*resolution` | Name resolution error |
| `socket.*error` | Socket error |
| `ssl.*error` | SSL/TLS error |
| `eof.*occurred` | Unexpected EOF |
| `broken.*pipe` | Broken pipe |
| `network.*unreachable` | Network unreachable |
| `host.*unreachable` | Host unreachable |

### HTTP Status Code Handling

| Status | Category | Behavior |
| ------ | -------- | -------- |
| 429 | `TRANSIENT` | Retry forever |
| 500-599 | `TRANSIENT` | Retry forever |
| 401 | `FATAL` | No retry |
| 403 | `FATAL` | No retry |

---

## Structured Output

### structured(schema, stream, *, fallbacks, auto_correct, retry, on_validation_error, on_auto_correct, on_event, adapter)

Guaranteed valid JSON matching a Pydantic schema.

```python
from pydantic import BaseModel
import l0

class UserProfile(BaseModel):
    name: str
    age: int
    email: str
    tags: list[str] = []

result = await l0.structured(
    schema=UserProfile,
    stream=lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": "Generate user data as JSON"}],
        stream=True,
    ),
    auto_correct=True,  # Fix common JSON errors
)

# Type-safe access
print(result.data.name)    # str
print(result.data.age)     # int
print(result.data.email)   # str
print(result.data.tags)    # list[str]
```

**Parameters:**

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `schema` | `type[BaseModel]` | required | Pydantic model class |
| `stream` | `AsyncIterator \| Callable[[], AsyncIterator]` | required | Async LLM stream or factory returning one |
| `fallbacks` | `list[AsyncIterator \| Callable]` | `None` | Fallback streams to try if primary fails |
| `auto_correct` | `bool` | `True` | Auto-fix common JSON errors |
| `strict_mode` | `bool` | `False` | Reject unknown fields in output |
| `retry` | `Retry` | `None` | Retry configuration for validation failures |
| `timeout` | `Timeout` | `None` | Timeout configuration (initial_token, inter_token) |
| `detect_zero_tokens` | `bool` | `False` | Detect zero-token outputs |
| `monitoring` | `bool` | `False` | Enable telemetry collection |
| `on_validation_error` | `Callable[[ValidationError, int], None]` | `None` | Callback when validation fails (error, attempt) |
| `on_auto_correct` | `Callable[[AutoCorrectInfo], None]` | `None` | Callback when auto-correction is applied |
| `on_retry` | `Callable[[int, str], None]` | `None` | Callback when retry occurs (attempt, reason) |
| `on_event` | `Callable[[ObservabilityEvent], None]` | `None` | Callback for observability events |
| `adapter` | `Any \| str` | `None` | Adapter hint ("openai", "litellm", or instance) |

### JSON Auto-Correction

```python
from l0._utils import auto_correct_json, extract_json_from_markdown

# Remove trailing commas
auto_correct_json('{"a": 1,}')  # '{"a": 1}'

# Balance braces
auto_correct_json('{"a": {"b": 1}')  # '{"a": {"b": 1}}'

# Balance brackets
auto_correct_json('[1, 2, 3')  # '[1, 2, 3]'

# Strip whitespace
auto_correct_json('  {"a": 1}  ')  # '{"a": 1}'

# Extract from markdown fences
extract_json_from_markdown('''
Here's the data:
```json
{"key": "value"}
```
''')  # '{"key": "value"}'
```

---

## Fallback Models

Sequential fallback when primary model fails:

```python
result = await l0.run(
    stream=lambda: openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        stream=True,
    ),
    fallbacks=[
        # Fallback 1: Cheaper OpenAI model
        lambda: openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        ),
        # Fallback 2: Different provider via LiteLLM
        lambda: litellm.acompletion(
            model="anthropic/claude-3-haiku-20240307",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        ),
    ],
)

# Check which model succeeded
if result.state.fallback_index == 0:
    print("Primary model (gpt-4o) succeeded")
elif result.state.fallback_index == 1:
    print("Fallback 1 (gpt-4o-mini) succeeded")
else:
    print(f"Fallback {result.state.fallback_index} succeeded")
```

### Fallback Behavior

1. Primary stream fails (error, timeout, guardrail violation)
2. L0 exhausts retries for primary stream
3. Moves to first fallback, resets retry counter
4. Repeats until success or all fallbacks exhausted
5. Raises last error if all fail

---

## Guardrails

### Built-in Rules

```python
import l0

# Individual rules (via Guardrails scoped API)
l0.Guardrails.json()           # Validates JSON structure (balanced braces)
l0.Guardrails.strict_json()    # Validates complete JSON (on completion only)
l0.Guardrails.pattern()        # Detects "As an AI..." patterns
l0.Guardrails.zero_output()    # Detects empty output
l0.Guardrails.stall()          # Detects token stalls
l0.Guardrails.repetition()     # Detects model looping
```

### Presets (Recommended)

```python
import l0

# Recommended: json + pattern + zero_output
guardrails = l0.Guardrails.recommended()

# Strict: All rules including drift detection
guardrails = l0.Guardrails.strict()

# JSON only
guardrails = l0.Guardrails.json_only()

# None (empty list)
guardrails = l0.Guardrails.none()
```

### Rule Details

| Rule | Streaming | Default Severity | Description |
| ---- | --------- | ---------------- | ----------- |
| `Guardrails.json()` | Yes | error | Checks balanced `{}[]` brackets |
| `Guardrails.strict_json()` | No | error | Validates JSON via `json.loads()` on complete |
| `Guardrails.pattern(patterns)` | Yes | warning | Regex patterns (default: AI slop) |
| `Guardrails.zero_output()` | No | error | Empty output on complete |
| `Guardrails.stall(max_gap)` | Yes | warning | No tokens for `max_gap` seconds |
| `Guardrails.repetition(window, threshold)` | Yes | error | Repeated content detection |

### Custom Guardrails

```python
from l0 import GuardrailRule, GuardrailViolation
from l0.types import State

def max_length_rule(limit: int = 1000) -> GuardrailRule:
    """Detect output exceeding length limit."""
    
    def check(state: State) -> list[GuardrailViolation]:
        if len(state.content) > limit:
            return [GuardrailViolation(
                rule="max_length",
                message=f"Output exceeds {limit} chars",
                severity="error",
                recoverable=True,
            )]
        return []
    
    return GuardrailRule(
        name="max_length",
        check=check,
        description="Detects output exceeding length limit",
        streaming=True,
        severity="error",
        recoverable=True,
    )

# Usage
result = await l0.run(
    stream=my_stream,
    guardrails=[max_length_rule(500)],
)
```

### GuardrailRule

```python
@dataclass
class GuardrailRule:
    name: str                                    # Unique name
    check: Callable[[State], list[GuardrailViolation]]
    description: str | None = None               # Human description
    streaming: bool = True                       # Check during streaming
    severity: Severity = "error"                 # Default severity
    recoverable: bool = True                     # Can retry on violation
```

### GuardrailViolation

```python
@dataclass
class GuardrailViolation:
    rule: str                         # Rule name that triggered
    message: str                      # Human-readable message
    severity: Severity                # "warning" | "error" | "fatal"
    recoverable: bool = True          # Can retry/fallback
    position: int | None = None       # Position in content
    timestamp: float | None = None    # When detected
    context: dict[str, Any] | None = None   # Extra context
    suggestion: str | None = None     # Suggested fix
```

### Violation Handling

```python
# Access violations from result
for violation in result.state.violations:
    print(f"[{violation.severity}] {violation.rule}: {violation.message}")
    
    if not violation.recoverable:
        print("  Fatal - cannot retry")
```

---

## Consensus

### consensus(tasks, strategy)

Multi-generation consensus for high-confidence results.

```python
import l0

result = await l0.consensus(
    tasks=[
        lambda: generate_answer_model_a(),
        lambda: generate_answer_model_b(),
        lambda: generate_answer_model_c(),
    ],
    strategy="majority",  # "unanimous" | "majority" | "best"
)
```

**Parameters:**

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `tasks` | `list[Callable[[], Awaitable[T]]]` | required | Async callables (minimum 2) |
| `strategy` | `Strategy` | `"majority"` | Consensus strategy (`"unanimous"`, `"majority"`, `"weighted"`, `"best"`) |
| `threshold` | `float` | `0.8` | Similarity threshold for matching |
| `resolve_conflicts` | `ConflictResolution` | `"vote"` | How to resolve disagreements (`"vote"`, `"merge"`, `"best"`, `"fail"`) |
| `weights` | `list[float]` | `None` | Weights for each task (for weighted strategy) |
| `minimum_agreement` | `float` | `0.6` | Minimum agreement ratio required |
| `schema` | `type[BaseModel]` | `None` | Pydantic schema for structured consensus |
| `on_event` | `Callable[[ObservabilityEvent], None]` | `None` | Callback for observability events |

### Strategies

| Strategy | Description | Raises |
| -------- | ----------- | ------ |
| `unanimous` | All results must be identical | `ValueError` if any differ |
| `majority` | Most common result wins (>50%) | `ValueError` if no majority |
| `best` | Return first result | Never (unless all fail) |

### Example: Multi-Model Validation

```python
async def get_answer(model: str) -> str:
    result = await l0.run(
        stream=lambda: client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": question}],
            stream=True,
        ),
    )
    return await result.read()

# Require agreement from multiple models
try:
    answer = await l0.consensus(
        tasks=[
            lambda: get_answer("gpt-4o"),
            lambda: get_answer("gpt-4o-mini"),
            lambda: get_answer("gpt-4-turbo"),
        ],
        strategy="majority",
    )
    print(f"Consensus answer: {answer}")
except ValueError as e:
    print(f"No consensus: {e}")
```

---

## Parallel Operations

### parallel(tasks, concurrency)

Run tasks with concurrency limit.

```python
import l0

async def process_document(doc: str) -> str:
    result = await l0.run(stream=lambda: summarize(doc))
    return await result.read()

# Process 10 documents, max 3 concurrent
results = await l0.parallel(
    tasks=[lambda d=doc: process_document(d) for doc in documents],
    concurrency=3,
)
```

**Parameters:**

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `tasks` | `list[Callable[[], Awaitable[T]]]` | required | Async callables |
| `options` | `ParallelOptions` | `None` | Options object (alternative to kwargs) |
| `concurrency` | `int` | `5` | Max concurrent tasks |
| `fail_fast` | `bool` | `False` | Stop on first error |
| `on_progress` | `Callable[[int, int], None]` | `None` | Progress callback (completed, total) |
| `on_complete` | `Callable[[T, int], None]` | `None` | Task complete callback (result, index) |
| `on_error` | `Callable[[Exception, int], None]` | `None` | Task error callback (error, index) |

**Returns:** `ParallelResult[T]` - Results, errors, and statistics

### race(tasks)

Return first successful result, cancel remaining.

```python
import l0

# First model to respond wins
result = await l0.race([
    lambda: fast_but_expensive_model(),
    lambda: slow_but_cheap_model(),
    lambda: backup_model(),
])
```

**Behavior:**
1. All tasks start immediately
2. First to complete successfully is returned
3. All other tasks are cancelled
4. If first fails, does NOT wait for others

### batched(items, handler, batch_size)

Process items in batches.

```python
import l0

async def embed(text: str) -> list[float]:
    # Get embedding for single text
    return embedding

# Process 1000 texts in batches of 50
embeddings = await l0.batched(
    items=texts,  # 1000 texts
    handler=embed,
    batch_size=50,
)
# Result: 1000 embeddings in order
```

**Parameters:**

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `items` | `list[T]` | required | Items to process |
| `handler` | `Callable[[T], Awaitable[T]]` | required | Async handler |
| `batch_size` | `int` | `10` | Batch size |

### Pattern Comparison

| Pattern | Execution | Cost | Best For |
| ------- | --------- | ---- | -------- |
| `run()` with fallbacks | Sequential on failure | Low | High availability |
| `race()` | Parallel, first wins | High | Low latency |
| `parallel()` | Parallel with limit | Medium | Batch processing |
| `batched()` | Sequential batches | Low | Large datasets |
| `consensus()` | Parallel, vote | High | High reliability |

---

## Custom Adapters

### Adapter Protocol

```python
from typing import Protocol, Any, TypeVar
from collections.abc import AsyncIterator
from l0 import AdaptedEvent

AdapterChunkT = TypeVar("AdapterChunkT", covariant=True)
AdapterOptionsT = TypeVar("AdapterOptionsT", contravariant=True)

class Adapter(Protocol[AdapterChunkT, AdapterOptionsT]):
    name: str

    def detect(self, stream: Any) -> bool:
        """Return True if this adapter can handle the stream."""
        ...

    def wrap(
        self,
        stream: AsyncIterator[Any],
        options: AdapterOptionsT | None = None,
    ) -> AsyncIterator[AdaptedEvent[Any]]:
        """Wrap raw stream into AdaptedEvent stream."""
        ...
```

### Built-in Adapters

| Adapter | Auto-Detected | Description |
| ------- | ------------- | ----------- |
| `OpenAIAdapter` | Yes | OpenAI SDK streams |
| `LiteLLMAdapter` | Yes | LiteLLM streams (alias for OpenAI) |

### Creating Custom Adapters

```python
from collections.abc import AsyncIterator
from typing import Any
import l0
from l0 import Event, EventType, Adapters

class AnthropicAdapter:
    """Adapter for direct Anthropic SDK (if not using LiteLLM)."""
    name = "anthropic"
    
    def detect(self, stream: Any) -> bool:
        return "anthropic" in type(stream).__module__
    
    async def wrap(self, stream: Any) -> AsyncIterator[Event]:
        usage = None
        
        async for event in stream:
            event_type = getattr(event, "type", None)
            
            if event_type == "content_block_delta":
                delta = getattr(event, "delta", None)
                if delta and hasattr(delta, "text"):
                    yield Event(type=EventType.TOKEN, text=delta.text)
            
            elif event_type == "content_block_start":
                block = getattr(event, "content_block", None)
                if block and getattr(block, "type", None) == "tool_use":
                    yield Event(
                        type=EventType.TOOL_CALL,
                        data={
                            "id": getattr(block, "id", None),
                            "name": getattr(block, "name", None),
                        }
                    )
            
            elif event_type == "message_delta":
                msg_usage = getattr(event, "usage", None)
                if msg_usage:
                    usage = {
                        "input_tokens": getattr(msg_usage, "input_tokens", 0),
                        "output_tokens": getattr(msg_usage, "output_tokens", 0),
                    }
            
            elif event_type == "message_stop":
                yield Event(type=EventType.COMPLETE, usage=usage)

# Register for auto-detection
Adapters.register(AnthropicAdapter())
```

### Adapter Functions

```python
from l0 import Adapters

# Register custom adapter (takes priority over built-ins)
Adapters.register(MyAdapter())

# Explicitly detect adapter
adapter = Adapters.detect(stream)
print(adapter.name)

# Use specific adapter by name
result = await l0.run(
    stream=my_stream,
    adapter="openai",  # Force OpenAI adapter
)

# Use adapter instance directly
result = await l0.run(
    stream=my_stream,
    adapter=MyCustomAdapter(),
)
```

### Adapter Invariants

Adapters **MUST**:
- Preserve text exactly (no trimming, modification)
- Convert errors to error events (never throw from wrap)
- Emit `COMPLETE` event exactly once at end
- Handle empty/null content gracefully

---

## Observability

### EventBus

Central event bus for all L0 observability.

```python
from l0 import EventBus, ObservabilityEvent, ObservabilityEventType

def my_handler(event: ObservabilityEvent):
    print(f"[{event.type}] stream={event.stream_id}")
    print(f"  ts={event.ts}ms")
    print(f"  context={event.context}")  # User-provided context
    print(f"  meta={event.meta}")        # Event-specific data

# Create event bus
bus = EventBus(
    handler=my_handler,
    context={"service": "my-app", "request_id": "req-123"},
)

# Access stream ID (UUIDv7)
print(bus.stream_id)

# Emit custom events
bus.emit(ObservabilityEventType.CHECKPOINT_SAVED, checkpoint="...", token_count=100)
```

### Using with run()

```python
result = await l0.run(
    stream=my_stream,
    on_event=lambda e: print(f"[{e.type}] context={e.context} meta={e.meta}"),
    context={"request_id": "req-123", "user_id": "user-456"},
)
```

### ObservabilityEvent

```python
@dataclass
class ObservabilityEvent:
    type: ObservabilityEventType     # Event type
    ts: float                        # Unix epoch MILLISECONDS
    stream_id: str                   # UUIDv7 stream identifier
    context: dict[str, Any]          # User-provided context (request_id, tenant, etc.)
    meta: dict[str, Any]             # Event-specific metadata (attempt, reason, etc.)
```

### Event Types

```python
class ObservabilityEventType(str, Enum):
    # Session
    SESSION_START = "SESSION_START"
    ATTEMPT_START = "ATTEMPT_START"
    SESSION_END = "SESSION_END"
    SESSION_SUMMARY = "SESSION_SUMMARY"

    # Stream
    STREAM_INIT = "STREAM_INIT"
    STREAM_READY = "STREAM_READY"
    TOKEN = "TOKEN"

    # Adapter
    ADAPTER_DETECTED = "ADAPTER_DETECTED"
    ADAPTER_WRAP_START = "ADAPTER_WRAP_START"
    ADAPTER_WRAP_END = "ADAPTER_WRAP_END"

    # Timeout
    TIMEOUT_START = "TIMEOUT_START"
    TIMEOUT_RESET = "TIMEOUT_RESET"
    TIMEOUT_TRIGGERED = "TIMEOUT_TRIGGERED"

    # Network
    NETWORK_ERROR = "NETWORK_ERROR"
    NETWORK_RECOVERY = "NETWORK_RECOVERY"
    CONNECTION_DROPPED = "CONNECTION_DROPPED"
    CONNECTION_RESTORED = "CONNECTION_RESTORED"

    # Abort
    ABORT_REQUESTED = "ABORT_REQUESTED"
    ABORT_COMPLETED = "ABORT_COMPLETED"

    # Tool
    TOOL_REQUESTED = "TOOL_REQUESTED"
    TOOL_START = "TOOL_START"
    TOOL_RESULT = "TOOL_RESULT"
    TOOL_ERROR = "TOOL_ERROR"
    TOOL_COMPLETED = "TOOL_COMPLETED"

    # Guardrail
    GUARDRAIL_PHASE_START = "GUARDRAIL_PHASE_START"
    GUARDRAIL_PHASE_END = "GUARDRAIL_PHASE_END"
    GUARDRAIL_RULE_START = "GUARDRAIL_RULE_START"
    GUARDRAIL_RULE_RESULT = "GUARDRAIL_RULE_RESULT"
    GUARDRAIL_RULE_END = "GUARDRAIL_RULE_END"
    GUARDRAIL_CALLBACK_START = "GUARDRAIL_CALLBACK_START"
    GUARDRAIL_CALLBACK_END = "GUARDRAIL_CALLBACK_END"

    # Drift
    DRIFT_CHECK_RESULT = "DRIFT_CHECK_RESULT"
    DRIFT_CHECK_SKIPPED = "DRIFT_CHECK_SKIPPED"

    # Checkpoint
    CHECKPOINT_SAVED = "CHECKPOINT_SAVED"

    # Resume
    RESUME_START = "RESUME_START"

    # Retry
    RETRY_START = "RETRY_START"
    RETRY_ATTEMPT = "RETRY_ATTEMPT"
    RETRY_END = "RETRY_END"
    RETRY_GIVE_UP = "RETRY_GIVE_UP"
    RETRY_FN_START = "RETRY_FN_START"
    RETRY_FN_RESULT = "RETRY_FN_RESULT"
    RETRY_FN_ERROR = "RETRY_FN_ERROR"

    # Fallback
    FALLBACK_START = "FALLBACK_START"
    FALLBACK_MODEL_SELECTED = "FALLBACK_MODEL_SELECTED"
    FALLBACK_END = "FALLBACK_END"

    # Completion
    FINALIZATION_START = "FINALIZATION_START"
    FINALIZATION_END = "FINALIZATION_END"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"

    # Consensus
    CONSENSUS_START = "CONSENSUS_START"
    CONSENSUS_STREAM_START = "CONSENSUS_STREAM_START"
    CONSENSUS_STREAM_END = "CONSENSUS_STREAM_END"
    CONSENSUS_OUTPUT_COLLECTED = "CONSENSUS_OUTPUT_COLLECTED"
    CONSENSUS_ANALYSIS = "CONSENSUS_ANALYSIS"
    CONSENSUS_RESOLUTION = "CONSENSUS_RESOLUTION"
    CONSENSUS_END = "CONSENSUS_END"

    # Structured output
    PARSE_START = "PARSE_START"
    PARSE_END = "PARSE_END"
    PARSE_ERROR = "PARSE_ERROR"
    SCHEMA_VALIDATION_START = "SCHEMA_VALIDATION_START"
    SCHEMA_VALIDATION_END = "SCHEMA_VALIDATION_END"
    SCHEMA_VALIDATION_ERROR = "SCHEMA_VALIDATION_ERROR"
    AUTO_CORRECT_START = "AUTO_CORRECT_START"
    AUTO_CORRECT_END = "AUTO_CORRECT_END"
    # TS compatibility aliases
    STRUCTURED_PARSE_START = "STRUCTURED_PARSE_START"
    STRUCTURED_PARSE_END = "STRUCTURED_PARSE_END"
    STRUCTURED_PARSE_ERROR = "STRUCTURED_PARSE_ERROR"
    STRUCTURED_VALIDATION_START = "STRUCTURED_VALIDATION_START"
    STRUCTURED_VALIDATION_END = "STRUCTURED_VALIDATION_END"
    STRUCTURED_VALIDATION_ERROR = "STRUCTURED_VALIDATION_ERROR"
    STRUCTURED_AUTO_CORRECT_START = "STRUCTURED_AUTO_CORRECT_START"
    STRUCTURED_AUTO_CORRECT_END = "STRUCTURED_AUTO_CORRECT_END"

    # Continuation
    CONTINUATION_START = "CONTINUATION_START"
```

---

## Error Handling

### Error Categories

```python
class ErrorCategory(str, Enum):
    NETWORK = "network"      # Connection drops, DNS, SSL
    TRANSIENT = "transient"  # 429, 503 - temporary
    MODEL = "model"          # Model refused, malformed
    CONTENT = "content"      # Guardrail, drift
    PROVIDER = "provider"    # API errors
    FATAL = "fatal"          # Auth errors (401/403)
    INTERNAL = "internal"    # Bugs, internal errors
```

### categorize_error(error)

```python
from l0.errors import categorize_error
from l0.types import ErrorCategory

try:
    result = await l0.run(stream=my_stream)
except Exception as error:
    category = categorize_error(error)
    
    if category in (ErrorCategory.NETWORK, ErrorCategory.TRANSIENT):
        print("Transient error - would have retried")
    elif category == ErrorCategory.FATAL:
        print("Fatal error - check credentials")
    elif category == ErrorCategory.INTERNAL:
        print("Bug - please report")
```

### Error Category Behavior

| Category | Retries | Counts Toward Limit | Example |
| -------- | ------- | ------------------- | ------- |
| `NETWORK` | Forever | No | Connection reset |
| `TRANSIENT` | Forever | No | 429 rate limit |
| `MODEL` | Limited | Yes | Model refused |
| `CONTENT` | Limited | Yes | Guardrail violation |
| `PROVIDER` | Depends | Depends | API error |
| `FATAL` | Never | - | 401 unauthorized |
| `INTERNAL` | Never | - | Bug |

### TimeoutError

L0's timeout error with details:

```python
from l0 import TimeoutError

try:
    result = await l0.run(
        stream=my_stream,
        timeout=l0.Timeout(initial_token=1.0),
    )
    async for event in result:
        pass
except TimeoutError as e:
    print(e.timeout_type)     # "initial_token" or "inter_token"
    print(e.timeout_seconds)  # The timeout value that was exceeded
```

---

## State Machine

L0 includes a lightweight state machine for tracking runtime state. Useful for debugging and monitoring.

### RuntimeState

```python
from l0 import StateMachine, RuntimeState, RuntimeStates

# Use RuntimeState/RuntimeStates constants instead of string literals
class RuntimeState(str, Enum):
    INIT = "init"                           # Initial setup
    WAITING_FOR_TOKEN = "waiting_for_token" # Waiting for first chunk
    STREAMING = "streaming"                 # Receiving tokens
    TOOL_CALL_DETECTED = "tool_call_detected"  # Tool call in progress
    CONTINUATION_MATCHING = "continuation_matching"  # Buffering for overlap detection
    CHECKPOINT_VERIFYING = "checkpoint_verifying"    # Validating checkpoint
    RETRYING = "retrying"                   # About to retry same stream
    FALLBACK = "fallback"                   # Switching to fallback stream
    FINALIZING = "finalizing"               # Finalizing (final guardrails, etc.)
    COMPLETE = "complete"                   # Success
    ERROR = "error"                         # Failed

# RuntimeStates is an alias for RuntimeState
RuntimeStates = RuntimeState
```

### StateMachine

```python
from l0 import StateMachine, RuntimeState, create_state_machine

# Create a state machine
sm = StateMachine()
# Or use the factory function
sm = create_state_machine()

# Transition to a new state (use constants)
sm.transition(RuntimeState.STREAMING)

# Get current state
sm.get()  # RuntimeState.STREAMING

# Check if in one of multiple states
sm.is_(RuntimeState.STREAMING, RuntimeState.CONTINUATION_MATCHING)  # True

# Alternative method name
sm.is_state(RuntimeState.STREAMING)  # True

# Check if terminal (complete or error)
sm.is_terminal()  # False (True for COMPLETE or ERROR)

# Subscribe to state changes
def on_state_change(state: RuntimeState):
    print(f"State changed to: {state}")

unsubscribe = sm.subscribe(on_state_change)

# Get history for debugging
history = sm.get_history()
# [StateTransition(from_state=INIT, to_state=STREAMING, timestamp=1234567890.123), ...]

# Reset to initial state and clear history
sm.reset()

# Unsubscribe when done
unsubscribe()
```

### StateTransition

```python
@dataclass
class StateTransition:
    from_state: RuntimeState   # Previous state
    to_state: RuntimeState     # New state
    timestamp: float           # Unix timestamp of transition
```

### Scoped API

```python
from l0 import StateMachine

# Create via class method
sm = StateMachine.create()
```

---

## Metrics

Simple counters for runtime metrics. OpenTelemetry is opt-in via separate adapter.

### Metrics Class

```python
from l0 import Metrics, MetricsSnapshot, create_metrics

# Create a new metrics instance
metrics = Metrics()
# Or use the factory function
metrics = create_metrics()

# Available counters (all integers)
metrics.requests          # Total stream requests
metrics.tokens            # Total tokens processed
metrics.retries           # Total retry attempts
metrics.network_retry_count  # Network retries (subset of retries)
metrics.errors            # Total errors encountered
metrics.violations        # Guardrail violations
metrics.drift_detections  # Drift detections
metrics.fallbacks         # Fallback activations
metrics.completions       # Successful completions
metrics.timeouts          # Timeouts (initial + inter-token)

# Increment counters directly
metrics.requests += 1
metrics.tokens += 150
metrics.completions += 1

# Get snapshot (immutable copy)
snapshot: MetricsSnapshot = metrics.snapshot()
print(f"Total tokens: {snapshot.tokens}")
print(f"Success rate: {snapshot.completions / snapshot.requests * 100}%")

# Reset all counters to zero
metrics.reset()

# Serialize to dictionary
data = metrics.to_dict()
# {"requests": 10, "tokens": 1500, "retries": 2, ...}
```

### MetricsSnapshot

```python
@dataclass
class MetricsSnapshot:
    requests: int            # Total stream requests
    tokens: int              # Total tokens processed
    retries: int             # Total retry attempts
    network_retry_count: int # Network retries (subset)
    errors: int              # Total errors
    violations: int          # Guardrail violations
    drift_detections: int    # Drift detections
    fallbacks: int           # Fallback activations
    completions: int         # Successful completions
    timeouts: int            # Timeouts
```

### Global Metrics

```python
from l0 import Metrics, get_global_metrics, reset_global_metrics

# Get the global metrics singleton
global_metrics = Metrics.get_global()
# Or use the legacy function
global_metrics = get_global_metrics()

# Use global metrics
global_metrics.requests += 1

# Reset global metrics
Metrics.reset_global()
# Or use the legacy function
reset_global_metrics()
```

### Scoped API

```python
from l0 import Metrics

# Create via class method
metrics = Metrics.create()

# Access global instance via class method
global_metrics = Metrics.get_global()

# Reset global via class method
Metrics.reset_global()
```

---

## Async Checks

Non-blocking wrappers for guardrails and drift detection. Uses fast/slow path pattern to prevent blocking the event loop during streaming.

### How It Works

1. **Fast path**: Delta-only check or small content - runs synchronously and returns immediately
2. **Slow path**: Large content (>10KB) - defers via `asyncio.call_soon()` to avoid blocking event loop

This prevents guardrails/drift from causing token delays that could trigger false timeouts.

### Usage Pattern

The async check pattern is used internally by L0's runtime but can be leveraged for custom implementations:

```python
import asyncio
from l0 import Guardrails, State

async def process_with_async_guardrails(content: str, state: State):
    """Example of async guardrail pattern."""
    rules = Guardrails.recommended()
    
    # For small content, check synchronously (fast path)
    if len(content) < 10000:
        for rule in rules:
            violations = rule.check(state)
            if violations:
                return violations
        return []
    
    # For large content, defer to avoid blocking (slow path)
    loop = asyncio.get_event_loop()
    future = loop.create_future()
    
    def run_check():
        try:
            all_violations = []
            for rule in rules:
                all_violations.extend(rule.check(state))
            loop.call_soon_threadsafe(future.set_result, all_violations)
        except Exception as e:
            loop.call_soon_threadsafe(future.set_exception, e)
    
    loop.call_soon(run_check)
    return await future
```

### Benefits

- **No false timeouts**: Large guardrail checks don't block token processing
- **Responsive streaming**: Tokens continue flowing while checks run in background
- **Automatic optimization**: Small checks run inline, large checks are deferred

---

## Stream Utilities

### consume_stream(stream)

Consume stream and return full text.

```python
import l0

result = await l0.run(stream=my_stream)
text = await l0.consume_stream(result)
print(text)
```

### get_text(result)

Helper to get text from Stream result.

```python
import l0

result = await l0.run(stream=my_stream)
text = await l0.get_text(result)
print(text)
```

### Aborting Streams

```python
result = await l0.run(stream=my_stream)

# Start consuming
async for event in result:
    if should_stop(event):
        result.abort()
        break
    process(event)

# Check if aborted
print(result.state.aborted)  # True
```

---

## Utility Functions

### JSON Utilities

```python
from l0._utils import auto_correct_json, extract_json_from_markdown

# Fix common JSON errors
fixed = auto_correct_json('{"a": 1,}')  # '{"a": 1}'
fixed = auto_correct_json('{"a": {"b": 1}')  # '{"a": {"b": 1}}'
fixed = auto_correct_json('[1, 2')  # '[1, 2]'

# Extract JSON from markdown
json_str = extract_json_from_markdown('''
```json
{"key": "value"}
```
''')
```

### Debug Logging

```python
import l0

# Enable debug logging
l0.enable_debug()
# Outputs: [l0] DEBUG: Starting L0 stream: ...
```

---

## Types

### WrappedClient

```python
class WrappedClient:
    """Wrapped OpenAI/LiteLLM client with L0 reliability.
    
    Returned by l0.wrap(client). Mirrors the original client API
    but adds automatic reliability features.
    """
    
    chat: WrappedChat                         # chat.completions.create()
    
    @property
    def unwrapped(self) -> Any:
        """Access the underlying unwrapped client."""
        ...
    
    def with_options(
        self,
        *,
        guardrails: list[GuardrailRule] | None = None,
        retry: Retry | None = None,
        timeout: Timeout | None = None,
        continue_from_last_good_token: ContinuationConfig | bool | None = None,
        ...
    ) -> WrappedClient:
        """Create a new wrapped client with updated options."""
        ...
```

### Stream

```python
class Stream:
    """Async iterator result with state and abort attached."""
    
    state: State                              # Runtime state
    abort: Callable[[], None]                 # Abort the stream
    errors: list[Exception]                   # Errors encountered
    
    def __aiter__(self) -> Stream: ...
    async def __anext__(self) -> Event: ...
    async def __aenter__(self) -> Stream: ...
    async def __aexit__(...) -> bool: ...
    async def read(self) -> str:
        """Consume the stream and return the full text content."""
        ...
```

### LazyStream

```python
class LazyStream:
    """Lazy stream wrapper - no await needed on creation.
    
    Like httpx.AsyncClient() or aiohttp.ClientSession(), this returns
    immediately and only does async work when you iterate or read.
    """
    
    state: State                              # Runtime state (after started)
    errors: list[Exception]                   # Errors encountered
    
    def abort(self) -> None: ...
    def __aiter__(self) -> LazyStream: ...
    async def __anext__(self) -> Event: ...
    async def __aenter__(self) -> LazyStream: ...
    async def __aexit__(...) -> bool: ...
    async def read(self) -> str:
        """Consume the stream and return the full text content."""
        ...
```

### State

```python
@dataclass
class State:
    content: str = ""
    checkpoint: str = ""                      # Last known good slice for continuation
    token_count: int = 0
    model_retry_count: int = 0
    network_retry_count: int = 0
    fallback_index: int = 0
    violations: list[GuardrailViolation] = field(default_factory=list)
    drift_detected: bool = False
    completed: bool = False
    aborted: bool = False
    first_token_at: float | None = None
    last_token_at: float | None = None
    duration: float | None = None
    resumed: bool = False                     # Whether stream was resumed from checkpoint
    network_errors: list[Any] = field(default_factory=list)
    # Multimodal state
    data_outputs: list[DataPayload] = field(default_factory=list)
    last_progress: Progress | None = None

    # Continuation state (for observability)
    resume_point: str | None = None           # The checkpoint content used for resume
    resume_from: int | None = None            # Character offset where resume occurred
    continuation_used: bool = False           # Whether continuation was actually used
    deduplication_applied: bool = False       # Whether deduplication removed overlap
    overlap_removed: str | None = None        # The overlapping text that was removed
```

### Event

```python
@dataclass
class Event:
    type: EventType
    text: str | None = None                   # Token content
    data: dict[str, Any] | None = None        # Tool call / misc data
    payload: DataPayload | None = None        # Multimodal data payload
    progress: Progress | None = None          # Progress update
    error: Exception | None = None            # Error (for error events)
    usage: dict[str, int] | None = None       # Token usage
    timestamp: float | None = None            # Event timestamp

    # Type check properties
    @property
    def is_token(self) -> bool: ...
    @property
    def is_message(self) -> bool: ...
    @property
    def is_data(self) -> bool: ...
    @property
    def is_progress(self) -> bool: ...
    @property
    def is_tool_call(self) -> bool: ...
    @property
    def is_error(self) -> bool: ...
    @property
    def is_complete(self) -> bool: ...
```

### EventType

```python
class EventType(str, Enum):
    TOKEN = "token"
    MESSAGE = "message"
    DATA = "data"
    PROGRESS = "progress"
    TOOL_CALL = "tool_call"
    ERROR = "error"
    COMPLETE = "complete"
```

### Retry

```python
@dataclass
class Retry:
    attempts: int | None = None                   # Model errors only
    max_retries: int | None = None                # Absolute cap
    base_delay: float | None = None               # Seconds
    max_delay: float | None = None                # Seconds
    strategy: BackoffStrategy | None = None       # Backoff strategy
    error_type_delays: ErrorTypeDelays | None = None  # Per-error-type delays
    retry_on: list[RetryableErrorType] | None = None  # Which error types to retry
    should_retry: Callable[..., bool | Coroutine] | None = None  # Veto callback
    calculate_delay: Callable[..., float] | None = None  # Custom delay calculation
```

> **Note:** All fields default to `None` and are populated via `__post_init__` from `RETRY_DEFAULTS`. Effective defaults: `attempts=3`, `max_retries=6`, `base_delay=1.0`, `max_delay=10.0`, `strategy=BackoffStrategy.FIXED_JITTER`.

### Timeout

```python
@dataclass
class Timeout:
    initial_token: int = 5000         # Milliseconds to first token
    inter_token: int = 10000          # Milliseconds between tokens
```

### GuardrailRule

```python
@dataclass
class GuardrailRule:
    name: str
    check: Callable[[State], list[GuardrailViolation]]
    description: str | None = None
    streaming: bool = True
    severity: Severity = "error"
    recoverable: bool = True
```

### GuardrailViolation

```python
@dataclass
class GuardrailViolation:
    rule: str
    message: str
    severity: Severity
    recoverable: bool = True
    position: int | None = None
    timestamp: float | None = None
    context: dict[str, Any] | None = None
    suggestion: str | None = None
```

### Severity

```python
Severity = Literal["warning", "error", "fatal"]
```

### BackoffStrategy

```python
class BackoffStrategy(str, Enum):
    EXPONENTIAL = "exponential"    # delay * 2^attempt
    LINEAR = "linear"              # delay * (attempt + 1)
    FIXED = "fixed"                # constant delay
    FULL_JITTER = "full-jitter"    # random(0, exponential)
    FIXED_JITTER = "fixed-jitter"  # base/2 + random(base/2)
```

### ErrorCategory

```python
class ErrorCategory(str, Enum):
    NETWORK = "network"
    TRANSIENT = "transient"
    MODEL = "model"
    CONTENT = "content"
    PROVIDER = "provider"
    FATAL = "fatal"
    INTERNAL = "internal"
```

### RuntimeState

```python
class RuntimeState(str, Enum):
    INIT = "init"
    WAITING_FOR_TOKEN = "waiting_for_token"
    STREAMING = "streaming"
    TOOL_CALL_DETECTED = "tool_call_detected"
    CONTINUATION_MATCHING = "continuation_matching"
    CHECKPOINT_VERIFYING = "checkpoint_verifying"
    RETRYING = "retrying"
    FALLBACK = "fallback"
    FINALIZING = "finalizing"
    COMPLETE = "complete"
    ERROR = "error"
```

### StateTransition

```python
@dataclass
class StateTransition:
    from_state: RuntimeState
    to_state: RuntimeState
    timestamp: float
```

### StateMachine

```python
class StateMachine:
    def transition(self, next_state: RuntimeState) -> None: ...
    def get(self) -> RuntimeState: ...
    def is_(self, *states: RuntimeState) -> bool: ...
    def is_state(self, *states: RuntimeState) -> bool: ...
    def is_terminal(self) -> bool: ...
    def reset(self) -> None: ...
    def get_history(self) -> list[StateTransition]: ...
    def subscribe(self, listener: Callable[[RuntimeState], None]) -> Callable[[], None]: ...
    
    @classmethod
    def create(cls) -> StateMachine: ...
```

### MetricsSnapshot

```python
@dataclass
class MetricsSnapshot:
    requests: int
    tokens: int
    retries: int
    network_retry_count: int
    errors: int
    violations: int
    drift_detections: int
    fallbacks: int
    completions: int
    timeouts: int
```

### Metrics

```python
class Metrics:
    requests: int
    tokens: int
    retries: int
    network_retry_count: int
    errors: int
    violations: int
    drift_detections: int
    fallbacks: int
    completions: int
    timeouts: int
    
    def reset(self) -> None: ...
    def snapshot(self) -> MetricsSnapshot: ...
    def to_dict(self) -> dict[str, int]: ...
    
    @classmethod
    def create(cls) -> Metrics: ...
    @classmethod
    def get_global(cls) -> Metrics: ...
    @classmethod
    def reset_global(cls) -> None: ...
```

### RetryableErrorType

```python
class RetryableErrorType(str, Enum):
    ZERO_OUTPUT = "zero_output"
    GUARDRAIL_VIOLATION = "guardrail_violation"
    DRIFT = "drift"
    INCOMPLETE = "incomplete"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
```

### ErrorTypeDelays

```python
@dataclass
class ErrorTypeDelays:
    connection_dropped: float | None = None
    fetch_error: float | None = None
    econnreset: float | None = None
    econnrefused: float | None = None
    sse_aborted: float | None = None
    no_bytes: float | None = None
    partial_chunks: float | None = None
    runtime_killed: float | None = None
    background_throttle: float | None = None
    dns_error: float | None = None
    ssl_error: float | None = None
    timeout: float | None = None
    unknown: float | None = None
```

---

## Imports

### Main Import (Recommended)

```python
import l0

# Simple wrapping (no await needed!)
result = l0.wrap(stream, guardrails=l0.Guardrails.recommended())
text = await result.read()

# Or with retries/fallbacks
result = await l0.run(
    stream=lambda: create_stream(),
    fallbacks=[lambda: backup_stream()],
    guardrails=l0.Guardrails.recommended(),
)

async for event in result:
    if event.is_token:
        print(event.text, end="")
```

### Direct Imports

```python
from l0 import (
    # Core
    wrap,
    run,
    l0,  # Alias to run()
    Stream,
    LazyStream,
    WrappedClient,
    State,
    Event,
    EventType,
    
    # Retry & Timeout
    Retry,
    RetryDefaults,
    Timeout,
    TimeoutError,
    BackoffStrategy,
    RetryableErrorType,
    ErrorTypeDelays,
    ErrorTypeDelayDefaults,
    CheckIntervals,
    LifecycleCallbacks,
    RETRY_DEFAULTS,
    ERROR_TYPE_DELAY_DEFAULTS,
    MINIMAL_RETRY,
    RECOMMENDED_RETRY,
    STRICT_RETRY,
    EXPONENTIAL_RETRY,
    
    # Guardrails (scoped API - use Guardrails.json(), Guardrails.pattern(), etc.)
    Guardrails,  # Class with .recommended(), .strict(), .json(), .pattern(), etc.
    GuardrailRule,
    GuardrailViolation,
    Violation,  # Alias for GuardrailViolation
    pattern_rule,
    custom_pattern_rule,
    JsonAnalysis,
    MarkdownAnalysis,
    LatexAnalysis,
    
    # Structured output
    structured,
    structured_stream,
    structured_object,
    structured_array,
    StructuredResult,
    StructuredStreamResult,
    StructuredConfig,
    StructuredState,
    StructuredTelemetry,
    AutoCorrectInfo,
    MINIMAL_STRUCTURED,
    RECOMMENDED_STRUCTURED,
    STRICT_STRUCTURED,
    
    # Parallel operations
    parallel,
    race,
    sequential,
    batched,
    Parallel,
    ParallelResult,
    ParallelOptions,
    RaceResult,
    AggregatedTelemetry,
    
    # Consensus
    Consensus,
    consensus,
    ConsensusResult,
    ConsensusOutput,
    ConsensusAnalysis,
    ConsensusPreset,
    Agreement,
    Disagreement,
    DisagreementValue,
    FieldAgreement,
    FieldConsensus,
    FieldConsensusInfo,
    
    # Pipeline
    pipe,
    Pipeline,
    PipelineStep,
    PipelineOptions,
    PipelineResult,
    StepContext,
    StepResult,
    create_pipeline,
    create_step,
    chain_pipelines,
    parallel_pipelines,
    create_branch_step,
    FAST_PIPELINE,
    RELIABLE_PIPELINE,
    PRODUCTION_PIPELINE,
    
    # Pool
    OperationPool,
    PoolOptions,
    PoolStats,
    create_pool,
    
    # Adapters
    Adapters,
    Adapter,
    AdaptedEvent,
    OpenAIAdapter,
    OpenAIAdapterOptions,
    LiteLLMAdapter,
    
    # Observability
    EventBus,
    ObservabilityEvent,
    ObservabilityEventType,
    
    # Errors
    Error,
    ErrorCode,
    ErrorContext,
    ErrorCategory,
    FailureType,
    RecoveryStrategy,
    RecoveryPolicy,
    NetworkError,
    NetworkErrorType,
    NetworkErrorAnalysis,
    
    # Window
    Window,
    DocumentWindow,
    DocumentChunk,
    WindowConfig,
    WindowStats,
    ChunkProcessConfig,
    ChunkResult,
    ChunkingStrategy,
    ProcessingStats,
    ContextRestorationOptions,
    ContextRestorationStrategy,
    
    # Continuation
    Continuation,
    ContinuationConfig,
    DeduplicationOptions,
    OverlapResult,
    
    # Drift
    Drift,
    DriftDetector,
    DriftConfig,
    DriftResult,
    
    # State Machine
    StateMachine,
    RuntimeState,
    RuntimeStates,
    StateTransition,
    create_state_machine,
    
    # Metrics
    Metrics,
    MetricsSnapshot,
    create_metrics,
    get_global_metrics,
    reset_global_metrics,
    
    # Event Sourcing
    EventSourcing,
    EventStore,
    EventStoreWithSnapshots,
    InMemoryEventStore,
    EventRecorder,
    EventReplayer,
    EventEnvelope,
    RecordedEvent,
    RecordedEventType,
    Snapshot,
    SerializedError,
    ReplayResult,
    ReplayCallbacks,
    ReplayedState,
    ReplayComparison,
    StreamMetadata,
    
    # Monitoring
    Monitoring,
    OpenTelemetry,
    OpenTelemetryConfig,
    OpenTelemetryExporter,
    OpenTelemetryExporterConfig,
    Sentry,
    SentryConfig,
    SentryExporter,
    SentryExporterConfig,
    SemanticAttributes,
    
    # Formatting
    Format,
    
    # Text
    Text,
    NormalizeOptions,
    WhitespaceOptions,
    
    # Comparison
    Compare,
    Difference,
    DifferenceSeverity,
    DifferenceType,
    StringComparisonOptions,
    ObjectComparisonOptions,
    
    # JSON
    JSON,
    AutoCorrectResult,
    CorrectionType,
    
    # JSON Schema
    JSONSchema,
    JSONSchemaAdapter,
    JSONSchemaDefinition,
    JSONSchemaValidationError,
    JSONSchemaValidationFailure,
    JSONSchemaValidationSuccess,
    SimpleJSONSchemaAdapter,
    UnifiedSchema,
    
    # Multimodal
    Multimodal,
    ContentType,
    DataPayload,
    Progress,
    
    # Utilities
    consume_stream,
    get_text,
    enable_debug,
    
    # Version
    __version__,
)
```

### Public Exports

| Category | Exports |
| -------- | ------- |
| Core | `wrap`, `run`, `l0` (alias), `Stream`, `LazyStream`, `WrappedClient`, `State`, `Event`, `EventType` |
| Retry & Timeout | `Retry`, `RetryDefaults`, `Timeout`, `TimeoutError`, `BackoffStrategy`, `RetryableErrorType`, `ErrorTypeDelays`, `ErrorTypeDelayDefaults`, `CheckIntervals`, `LifecycleCallbacks`, `RETRY_DEFAULTS`, `ERROR_TYPE_DELAY_DEFAULTS`, `MINIMAL_RETRY`, `RECOMMENDED_RETRY`, `STRICT_RETRY`, `EXPONENTIAL_RETRY` |
| Continuation | `Continuation`, `ContinuationConfig`, `DeduplicationOptions`, `OverlapResult` |
| Errors | `Error`, `ErrorCode`, `ErrorContext`, `ErrorCategory`, `FailureType`, `RecoveryStrategy`, `RecoveryPolicy`, `NetworkError`, `NetworkErrorType`, `NetworkErrorAnalysis` |
| Guardrails | `Guardrails`, `GuardrailRule`, `GuardrailViolation`, `Violation`, `pattern_rule`, `custom_pattern_rule`, `JsonAnalysis`, `MarkdownAnalysis`, `LatexAnalysis` |
| Structured | `structured`, `structured_stream`, `structured_object`, `structured_array`, `StructuredResult`, `StructuredStreamResult`, `StructuredConfig`, `StructuredState`, `StructuredTelemetry`, `AutoCorrectInfo`, `MINIMAL_STRUCTURED`, `RECOMMENDED_STRUCTURED`, `STRICT_STRUCTURED` |
| Parallel | `parallel`, `race`, `sequential`, `batched`, `Parallel`, `ParallelResult`, `ParallelOptions`, `RaceResult`, `AggregatedTelemetry` |
| Pipeline | `pipe`, `Pipeline`, `PipelineStep`, `PipelineOptions`, `PipelineResult`, `StepContext`, `StepResult`, `create_pipeline`, `create_step`, `chain_pipelines`, `parallel_pipelines`, `create_branch_step`, `FAST_PIPELINE`, `RELIABLE_PIPELINE`, `PRODUCTION_PIPELINE` |
| Pool | `OperationPool`, `PoolOptions`, `PoolStats`, `create_pool` |
| Consensus | `Consensus`, `consensus`, `ConsensusResult`, `ConsensusOutput`, `ConsensusAnalysis`, `ConsensusPreset`, `Agreement`, `Disagreement`, `DisagreementValue`, `FieldAgreement`, `FieldConsensus`, `FieldConsensusInfo` |
| Adapters | `Adapters`, `Adapter`, `AdaptedEvent`, `OpenAIAdapter`, `OpenAIAdapterOptions`, `LiteLLMAdapter` |
| Observability | `EventBus`, `ObservabilityEvent`, `ObservabilityEventType` |
| Window | `Window`, `DocumentWindow`, `DocumentChunk`, `WindowConfig`, `WindowStats`, `ChunkProcessConfig`, `ChunkResult`, `ChunkingStrategy`, `ProcessingStats`, `ContextRestorationOptions`, `ContextRestorationStrategy` |
| Drift | `Drift`, `DriftDetector`, `DriftConfig`, `DriftResult` |
| State Machine | `StateMachine`, `RuntimeState`, `RuntimeStates`, `StateTransition`, `create_state_machine` |
| Metrics | `Metrics`, `MetricsSnapshot`, `create_metrics`, `get_global_metrics`, `reset_global_metrics` |
| Event Sourcing | `EventSourcing`, `EventStore`, `EventStoreWithSnapshots`, `InMemoryEventStore`, `EventRecorder`, `EventReplayer`, `EventEnvelope`, `RecordedEvent`, `RecordedEventType`, `Snapshot`, `SerializedError`, `ReplayResult`, `ReplayCallbacks`, `ReplayedState`, `ReplayComparison`, `StreamMetadata` |
| Monitoring | `Monitoring`, `OpenTelemetry`, `OpenTelemetryConfig`, `OpenTelemetryExporter`, `OpenTelemetryExporterConfig`, `Sentry`, `SentryConfig`, `SentryExporter`, `SentryExporterConfig`, `SemanticAttributes` |
| Formatting | `Format` |
| Text | `Text`, `NormalizeOptions`, `WhitespaceOptions` |
| Comparison | `Compare`, `Difference`, `DifferenceSeverity`, `DifferenceType`, `StringComparisonOptions`, `ObjectComparisonOptions` |
| JSON | `JSON`, `AutoCorrectResult`, `CorrectionType` |
| JSON Schema | `JSONSchema`, `JSONSchemaAdapter`, `JSONSchemaDefinition`, `JSONSchemaValidationError`, `JSONSchemaValidationFailure`, `JSONSchemaValidationSuccess`, `SimpleJSONSchemaAdapter`, `UnifiedSchema` |
| Multimodal | `Multimodal`, `ContentType`, `DataPayload`, `Progress` |
| Utilities | `consume_stream`, `get_text`, `enable_debug` |
| Version | `__version__` |

---

## See Also

- [README.md](./README.md) - Quick start guide
- [ADVANCED.md](./ADVANCED.md) - Advanced usage and full examples
