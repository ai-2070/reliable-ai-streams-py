# L0: Deterministic Streaming Execution Substrate for AI

**A reliability, observability, and recovery layer for token streams**

> LLMs produce high-value reasoning over a low-integrity transport layer. Streams stall, drop tokens, reorder events, violate timing guarantees, and expose no deterministic contract. L0 fixes the transport so you can build reliable systems on top of any AI stream.

---

## Abstract

Modern LLM applications increasingly depend on _streaming_ responses: chat UIs, agent runtimes, tool-call orchestration, real-time summarization, structured extraction, and multimodal generation. But today's provider streams are not a reliable substrate. They are best-effort event feeds whose failure modes make production reliability, auditability, and reproducibility expensive and fragile.

**L0** (`ai2070-l0`) is a deterministic streaming execution substrate that wraps existing model streams and upgrades them into a contract you can build systems on. It provides token-level normalization, smart retries with error-category-aware backoff, streaming guardrails, drift detection, checkpoint-based resumption, model fallbacks, multi-model consensus, structured output validation, streaming pipelines, document windowing, event sourcing with deterministic replay, and built-in telemetry with OpenTelemetry and Sentry integrations.

L0 is provider-agnostic. Built-in adapters support OpenAI, Anthropic, and LiteLLM (100+ providers), with an extensible adapter registry for custom providers. It handles text, structured JSON, and multimodal streams (image, audio, video) under the same deterministic contract.

Available in Python (`uv add ai2070-l0`) and TypeScript (`npm install @ai2070/l0`) with full lifecycle and event signature parity.

---

## The Problem: High-Value Reasoning on a Low-Integrity Transport

Streaming is where most production LLM failures actually happen. Even when the model itself is working correctly, the stream can:

- **Stall**: no first token arrives, or long gaps open between tokens, with no signal to distinguish "thinking" from "dead."
- **Disconnect mid-stream**: generation halts at token 1500, yielding partial output with no built-in way to resume.
- **Reorder or drop chunks**: out-of-order sequences or missing segments produce garbled output.
- **Return empty responses**: structurally valid but semantically void payloads slip past naive error checks.
- **Degrade format**: output shifts from well-formed JSON or Markdown into broken, ambiguous fragments - fences left open, braces unmatched, tables malformed.
- **Drift semantically**: tone shifts, hedging spirals, repetition loops, or meta-commentary appears mid-generation, breaking downstream consumers.
- **Fail silently**: provider-specific behaviors lack sufficient visibility or hooks for debugging.

The result: retries become guesswork, supervision becomes fuzzy, and reproducibility becomes nearly impossible. Every team that ships LLM-powered features eventually builds ad-hoc versions of these protections. L0 is the systematic answer.

---

## Thesis

A robust LLM stack needs something analogous to a database's transaction log or a distributed system's consensus layer. Specifically, it needs:

- A **deterministic lifecycle** that does not vary by provider.
- An **explicit error taxonomy** that drives recovery decisions.
- **Streaming-safe validation** that checks output as it arrives, not just after.
- **Replayable execution** for debugging, auditing, and testing.
- **First-class telemetry** as a built-in output, not an afterthought.
- **Recovery primitives designed for streams** - not batch retry wrappers bolted on.

L0 treats a model stream as a noisy transport and upgrades it into a deterministic, observable, recoverable runtime.

---

## Design Principles

1. **Determinism by contract.**
   Every execution follows the same lifecycle and emits a consistent event shape, independent of provider quirks. The lifecycle is specified precisely enough to be ported across language implementations with identical behavior.

2. **Bring your stream.**
   L0 adapts to OpenAI, Anthropic, LiteLLM (100+ providers), and custom sources via an adapter protocol. No vendor lock-in; no SDK replacement.

3. **Validate, don't rewrite.**
   Guardrails are pure functions. They detect violations and signal retry or halt - they never silently mutate content.

4. **Observability is a first-class output.**
   Telemetry is built in and returned alongside results. Optional integrations with OpenTelemetry and Sentry are available but not required.

5. **Performance headroom.**
   The substrate must stay far ahead of model inference speeds. L0 uses incremental state tracking, sliding-window analysis, and tunable check intervals. Even with the full feature stack enabled, throughput exceeds **108,000 tokens/second** in benchmarks - orders of magnitude above current inference speeds.

6. **Safety-first defaults.**
   Checkpoint continuation is off by default. Structured objects are never resumed mid-stream. No silent corruption. Every opt-in feature requires explicit enablement.

7. **Type safety.**
   Full strict-mode type checking (mypy), `py.typed` marker for downstream consumers, and every public API fully annotated.

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────┐
│                   Your Application                  │
└──────────────────────────┬──────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────┐
│                    L0 Layer (DSES)                   │
│                                                     │
│  ┌─────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐  │
│  │ Retries │ │Guardrails│ │ Drift  │ │Checkpoint│  │
│  └─────────┘ └──────────┘ └────────┘ └──────────┘  │
│  ┌─────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐  │
│  │Fallbacks│ │Consensus │ │Parallel│ │ Replay   │  │
│  └─────────┘ └──────────┘ └────────┘ └──────────┘  │
│  ┌─────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐  │
│  │Telemetry│ │Structured│ │  Tools │ │Multimodal│  │
│  └─────────┘ └──────────┘ └────────┘ └──────────┘  │
│                                                     │
│         Adapters: OpenAI · Anthropic · LiteLLM      │
└──────────────────────────┬──────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────┐
│              Provider Streams (Any LLM)              │
└─────────────────────────────────────────────────────┘
```

### Core Primitive

L0's entry point is `l0.run()`: you provide a _stream factory_ (a callable that returns an async iterator from any provider SDK), and L0 returns a normalized event stream plus final state, errors, and telemetry.

For simpler integration, `l0.wrap()` wraps an OpenAI or LiteLLM client directly, adding L0 reliability to every call transparently.

### Adapter Protocol

L0 uses an adapter protocol to normalize provider-specific stream formats into unified `Event` objects:

| Adapter | Handles |
|---|---|
| `OpenAIAdapter` | OpenAI SDK streams |
| `LiteLLMAdapter` | LiteLLM streams (OpenAI-compatible, 100+ providers) |
| `EventPassthroughAdapter` | Raw `Event` async iterators |

For custom providers, L0 provides an adapter registry and helper functions (`to_l0_events()`, `token_event()`, `complete_event()`, `error_event()`, `data_event()`) to convert any async iterable into L0's normalized event format.

---

## Deterministic Lifecycle

Every L0 execution follows a specified lifecycle:

```
START
  → stream events (TOKEN · DATA · PROGRESS · TOOL_CALL)
  → periodic hooks (checkpoint · guardrail · drift · timeout)
  → COMPLETE or ERROR
  → decision: RETRY · FALLBACK · RESUME · HALT
```

This lifecycle is observable through a comprehensive callback interface:

| Callback | Fires when |
|---|---|
| `on_start` | Attempt begins (includes retry/fallback flags) |
| `on_token` | Each text token arrives |
| `on_event` | Any normalized event |
| `on_checkpoint` | Checkpoint saved |
| `on_violation` | Guardrail violation detected |
| `on_drift` | Drift detected (with types and confidence) |
| `on_timeout` | TTFT or inter-token timeout triggered |
| `on_retry` | Retry initiated (with attempt number and reason) |
| `on_fallback` | Fallback stream activated (with index and reason) |
| `on_resume` | Resuming from checkpoint |
| `on_tool_call` | Tool call detected (name, ID, arguments) |
| `on_abort` | Stream aborted |
| `on_error` | Error occurred (with will_retry/will_fallback flags) |
| `on_complete` | Execution finished (with final state) |

The lifecycle is specified precisely enough that implementations in different languages produce identical callback sequences for the same input.

---

## Normalized Events and State

### Events

L0 normalizes every provider event into a unified `Event` type:

| Event Type | Description |
|---|---|
| `token` | A text token (the most common event) |
| `message` | A complete message chunk |
| `data` | Multimodal payload (image, audio, video, file) |
| `progress` | Progress update for long-running operations |
| `tool_call` | Streaming tool/function call with buffered arguments |
| `error` | An error event from the provider |
| `complete` | Stream completion signal |

### State

L0 maintains an internal `State` object that tracks the full execution context:

- **Content**: accumulated text, token count, content length.
- **Checkpoints**: last checkpoint content and token position.
- **Retries**: separate counters for model retries and network retries.
- **Fallbacks**: current fallback index.
- **Violations**: list of guardrail violations with rule, severity, and message.
- **Drift**: whether drift was detected and which types.
- **Timing**: first token timestamp, last token timestamp, total duration.
- **Network**: categorized network error details.
- **Multimodal**: data payloads and progress updates.
- **Tools**: detected tool calls with names, IDs, and arguments.

This state drives all recovery decisions and is available for post-run analysis.

---

## Reliability Layer

### Smart Retries with Error-Aware Routing

Not all failures deserve the same response. L0 classifies every error into one of seven categories and routes recovery accordingly:

| Category | Recovery | Counts toward model limit? |
|---|---|---|
| **network** | Retry with backoff | No (separate counter) |
| **transient** | Retry with limit | Yes |
| **model** | Retry with limit, may fallback | Yes |
| **content** | Retry if recoverable, may fallback | Yes |
| **provider** | Usually halt, may fallback | Yes |
| **fatal** | Always halt | - |
| **internal** | Always halt | - |

This separation is critical: a DNS failure or connection reset should retry aggressively without exhausting the budget reserved for model-level issues like guardrail violations.

L0 provides five backoff strategies:

- **Exponential**: `delay × base^attempt`, capped at max.
- **Linear**: `delay × attempt`, capped at max.
- **Fixed**: constant delay.
- **Full jitter**: `random(0, delay × base^attempt)`.
- **Fixed jitter**: `delay + random(0, delay)`.

Per-error-type delay overrides and custom `should_retry` callbacks give fine-grained control. Retry presets (`minimal`, `recommended`, `strict`, `exponential`) cover common configurations.

### Timeouts

Streaming has two critical timing windows:

- **Time-to-first-token (TTFT)**: how long to wait before the first token arrives.
- **Inter-token gap**: the maximum silence between consecutive tokens.

L0 enforces both independently and treats violations as recoverable/transient failures, triggering retry with backoff.

### Network Protection

L0 recognizes 15+ network failure patterns and applies category-correct recovery:

- Connection dropped, reset, refused
- SSE stream aborted
- No bytes received
- Partial chunk failures
- DNS resolution failures
- SSL/TLS errors
- Runtime killed / background throttled
- Timeout at transport layer

Each pattern is classified into the error taxonomy so retry behavior is automatic and correct.

### Zero-Token and Stall Protection

A subtle failure mode: the model produces nothing, or produces a few tokens then stops. L0 detects:

- **Zero output**: stream completes with no meaningful content.
- **Early termination**: stream closes far sooner than expected.
- **Mid-stream stalls**: tokens stop arriving but the stream does not close.

These are treated as recoverable failures, triggering retry or fallback automatically.

### Fallback Chains

When retries are exhausted, L0 falls through to a configurable sequence of fallback stream factories. This enables high-availability execution across models or providers while preserving a single deterministic contract to the application. Each fallback gets its own full retry budget.

---

## Guardrails: Streaming-Safe Validation

Guardrails in L0 are **pure validation functions**. They inspect streaming output, return violations with severity levels, and signal whether the runtime should retry or halt. They never rewrite content.

### Built-in Rules

| Rule | What it catches |
|---|---|
| **JSON** | Unclosed braces/brackets, invalid syntax, wrong root type. Strict mode enforces parseability. |
| **Markdown** | Unclosed fences, malformed tables, broken lists, sentences that end mid-word. |
| **LaTeX** | Unmatched `\begin`/`\end` environments, unclosed math delimiters. |
| **Zero output** | Empty or meaningless responses (whitespace-only, trivially short). |
| **Pattern** | Meta-commentary ("As an AI…"), instruction leak markers, placeholders, hedging spirals, refusal patterns, excessive repetition. |

### Presets

| Preset | Rules enabled |
|---|---|
| `MINIMAL_GUARDRAILS` | Zero output only |
| `RECOMMENDED_GUARDRAILS` | JSON structure + pattern detection |
| `STRICT_GUARDRAILS` | All formats + all patterns |
| `JSON_ONLY_GUARDRAILS` | JSON rules only |
| `MARKDOWN_ONLY_GUARDRAILS` | Markdown rules only |
| `LATEX_ONLY_GUARDRAILS` | LaTeX rules only |

### Fast Path / Slow Path

To avoid blocking the token loop, L0 splits guardrail execution:

- **Fast path**: checks only the new delta (< 1 KB). Runs synchronously on every token (or per configurable interval).
- **Slow path**: full-content scans deferred to async when accumulated output exceeds 5 KB.

This keeps streaming responsive while still catching structural violations that only manifest across the full output.

### Severity

Violations carry one of three severity levels:

| Severity | Runtime behavior |
|---|---|
| `warning` | Recorded, execution continues |
| `error` | Triggers retry (if budget remains) |
| `fatal` | Halts immediately |

---

## Drift Detection

Even when output is structurally valid, it can drift in ways that break downstream usage. L0 detects seven categories of semantic drift:

| Drift Type | Signal |
|---|---|
| **tone_shift** | Unexpected change in voice or register |
| **meta_commentary** | Model starts talking about itself or the task |
| **format_collapse** | Structured output degrades into prose |
| **markdown_collapse** | Markdown formatting breaks down |
| **repetition** | Phrases or sentences loop |
| **entropy_spike** | Statistical surprise in token distribution |
| **hedging** | Excessive qualification or uncertainty language |

For performance, drift analysis operates over a **sliding window** (default 500 characters) rather than rescanning the full output - keeping cost at O(window_size) regardless of total output length. Thresholds for entropy and repetition are configurable. When drift is detected, L0 can trigger a retry with the drift types and confidence score reported via callback.

---

## Checkpoints and Resumption

If a stream disconnects at token 1500, restarting from zero is wasteful. L0 supports opt-in checkpoint-based resumption that continues from the **last known good position**.

### How It Works

1. L0 periodically saves checkpoints at a configurable token interval.
2. On retry or fallback, L0 validates the checkpoint against guardrails and drift detection.
3. If the checkpoint is clean, L0 replays the checkpoint content and optionally constructs a continuation prompt instructing the model to pick up where it left off.

### Overlap Deduplication

When models continue from a checkpoint, they frequently repeat words from the boundary. L0 includes automatic suffix/prefix overlap detection and deduplication, with configurable sensitivity, minimum overlap threshold, case sensitivity, and whitespace normalization. This is enabled by default when continuation is active.

### Limitation

Checkpoint continuation is **not recommended for structured JSON output**, because prepending partial JSON to a continuation prompt can corrupt the structure. For structured flows, retry from scratch is safer and is the default behavior.

---

## Structured Output

For machine-readable output, L0 provides schema-validated structured extraction:

- **Pydantic validation**: validate streamed output against Pydantic models or JSON Schema.
- **Auto-correction**: common truncation issues (missing closing braces/brackets, trailing commas, unclosed strings, Markdown code fences wrapping JSON) are automatically repaired.
- **Streaming + end validation**: stream tokens to a live UI while enforcing a final correctness contract on completion.
- **Strict mode**: rejects unknown fields.
- **Retry on validation failure**: schema violations trigger retry with the same error-aware routing as other failures.

Structured presets (`minimal`, `recommended`, `strict`) combine schema validation with appropriate guardrail configurations.

The structured API includes `structured()`, `structured_object()`, `structured_array()`, and `structured_stream()` entry points for different use cases.

---

## Multi-Model Consensus

Some tasks benefit from comparing independent generations. L0's consensus primitive runs multiple streams in parallel, compares their outputs, and resolves disagreements:

| Strategy | Behavior |
|---|---|
| `unanimous` | All outputs must agree |
| `majority` | Most common output wins |
| `weighted` | Custom weights per output |
| `best` | Highest quality by scoring function |

When outputs disagree, L0 applies a configurable conflict resolution mode:

| Resolution | Behavior |
|---|---|
| `vote` | Majority vote among conflicting values |
| `merge` | Merge conflicting outputs intelligently |
| `best` | Select the highest-quality conflicting value |
| `fail` | Fail on conflict (strictest) |

For structured outputs, consensus can operate field-by-field with agreement classification (`exact`, `similar`, `structural`, `semantic`) and disagreement severity (`minor`, `moderate`, `major`, `critical`). Presets (`strict`, `standard`, `lenient`, `best`) cover common configurations.

---

## Parallel Execution Patterns

L0 provides composable patterns for multi-stream orchestration:

- **Race**: launch multiple streams in parallel, keep the first valid result. Ideal for latency-sensitive paths.
- **Parallel**: fan-out with configurable concurrency limits, fan-in all results. For batch processing or multi-aspect extraction.
- **Sequential**: ordered execution with result threading between stages.
- **Pool**: operation pooling with resource management for sustained workloads.
- **Pipeline**: multi-phase streaming workflows where each stage transforms the output of the previous. Supports conditional branching, step-level error handling, and chaining/parallelizing multiple pipelines.

Pipeline presets: `FAST_PIPELINE` (fail fast), `RELIABLE_PIPELINE` (graceful failures), `PRODUCTION_PIPELINE` (timeouts + graceful).

All patterns integrate with L0's retry, fallback, and telemetry systems.

---

## Document Windows

For long documents that exceed model context limits, L0 provides built-in chunking with context preservation:

| Strategy | Behavior |
|---|---|
| `token` | Token count-based chunking |
| `char` | Character count-based chunking |
| `paragraph` | Respect paragraph boundaries |
| `sentence` | Respect sentence boundaries |

Presets: `Window.small()` (1000 tokens, 100 overlap), `Window.medium()` (2000 tokens, 200 overlap), `Window.large()` (4000 tokens, 400 overlap), `Window.paragraph()`, `Window.sentence()`.

Windows support parallel chunk processing (`process_all(concurrency=...)`) and sequential processing, with context restoration strategies (`adjacent`, `overlap`, `full`) for maintaining coherence across chunk boundaries.

---

## Tool Call Support

L0 provides first-class support for streaming tool calls (function calling):

- Detects tool call events as they stream and buffers arguments incrementally.
- Emits `tool_call` events with the tool name, call ID, and accumulated arguments.
- Reports tool calls via the `on_tool_call` lifecycle callback.
- Tracks all detected tool calls in the execution state.

This enables agent runtimes to react to tool calls in real time while still benefiting from L0's full reliability stack.

---

## Multimodal Support

L0 handles non-text outputs through a unified multimodal system:

- **Content types**: image, audio, video, file, JSON, binary.
- **Encoding**: base64 inline data or URL references.
- **Metadata**: dimensions, duration, model, seed, and custom fields.
- **Progress events**: long-running generation (e.g., image synthesis) reports progress updates.

Multimodal payloads are tracked in execution state and included in event sourcing recordings.

---

## JSON Auto-Healing and Format Repair

LLM output frequently arrives with structural defects. L0 provides automatic repair utilities:

- **JSON**: missing closing braces/brackets/quotes, trailing commas, duplicate quotes, extraction from surrounding prose or Markdown fences, single-to-double quote conversion, comment stripping, control character escaping.
- **Markdown**: unterminated code fences.
- **Tool calls**: malformed function call arguments.

These repairs are applied only when explicitly enabled (`auto_correct=True`) and are tracked - corrections report exactly what was fixed, so repairs are never silent.

---

## Event Sourcing and Deterministic Replay

Reliability alone is insufficient for production systems. Debugging, auditing, and compliance demand reproducibility.

### Recording

L0's event sourcing system records every stream operation as an atomic event:

| Event Type | What it captures |
|---|---|
| `START` | Execution initiated |
| `TOKEN` | Individual token received |
| `CHECKPOINT` | Checkpoint saved |
| `GUARDRAIL` | Guardrail evaluation result |
| `DRIFT` | Drift detection result |
| `RETRY` | Retry initiated |
| `FALLBACK` | Fallback activated |
| `CONTINUATION` | Resumption from checkpoint |
| `COMPLETE` | Execution finished |
| `ERROR` | Error occurred |

Events are stored via an `EventStore` interface. L0 ships with `InMemoryEventStore` for testing and `EventStoreWithSnapshots` for fast recovery. Custom stores can be implemented for persistent storage.

### Replay

In replay mode, L0 performs **no network calls**, executes **no retries**, and runs **no recomputation** of guardrails or drift. It rehydrates the exact recorded events, producing deterministic reproduction. Lifecycle callbacks still fire during replay (for testing and debugging), but no side effects occur.

Replay supports configurable playback speed (0 = instant, 1 = real-time), partial replay via sequence ranges (`from_seq`/`to_seq`), token-only replay streams, and comparison between replays for consistency verification.

This enables:

- **Time-travel debugging**: step through a production failure locally.
- **Deterministic tests**: record once, replay forever - no flaky network dependencies.
- **Audit trails**: prove exactly what happened, token by token.

---

## Monitoring and Telemetry

L0 ships with built-in telemetry returned alongside every result:

- **Throughput**: tokens per second, total duration, token counts.
- **Timing**: time-to-first-token, inter-token latencies.
- **Retries**: attempt counts split by network vs. model, with reasons.
- **Guardrails**: violations by rule name and severity.
- **Drift**: events by type with confidence scores.
- **Network**: error types and frequencies.
- **Checkpoints**: continuation usage and checkpoint positions.
- **Tools**: detected tool calls.

### Simple Metrics

A lightweight counter-based metrics system (with a global singleton via `Metrics.get_global()`) tracks aggregate statistics across executions for dashboards and alerting.

### Optional Integrations

- **OpenTelemetry**: spans, metrics, and attributes for distributed tracing.
- **Sentry**: error tracking and performance monitoring.
- **Custom handlers**: compose multiple event handlers for domain-specific observability.

These are strictly optional - L0's built-in telemetry works without any external dependencies.

---

## Performance

L0 is designed to stay far ahead of model inference speeds, even with the full feature stack enabled.

### Benchmark Results (Apple M1 Max, Python 3.13, zero-delay mock streams)

| Scenario | Tokens/s | Avg Duration | TTFT | Overhead |
|---|---|---|---|---|
| Baseline (raw streaming) | 1,518,271 | 1.32 ms | 0.02 ms | - |
| L0 Core (no features) | 551,696 | 3.63 ms | 0.08 ms | 175% |
| L0 + JSON Guardrail | 469,922 | 4.26 ms | 0.07 ms | 223% |
| L0 + All Guardrails | 367,328 | 5.44 ms | 0.08 ms | 313% |
| L0 + Drift Detection | 119,758 | 16.70 ms | 0.08 ms | 1166% |
| **L0 Full Stack** | **108,257** | **18.48 ms** | **0.07 ms** | **1301%** |

### Key Optimizations

| Technique | Effect |
|---|---|
| Incremental JSON state tracking | O(delta) per token, not O(content) |
| Sliding-window drift detection | O(window_size), not O(content_length) |
| Fast/slow guardrail split | Heavy scans deferred to async |
| Tunable check intervals | Guardrails every 15 tokens, drift every 25, checkpoints every 20 |
| Lazy module loading | Only imported features incur startup cost |

### Inference Speed Headroom

Even with the full stack enabled, L0 sustains ~108K tokens/s - orders of magnitude above current and next-generation inference hardware:

| GPU Generation | Expected Tokens/s | L0 Headroom |
|---|---|---|
| Current (H100) | ~100-200 | 540-1,080x |
| Blackwell (B200) | ~1,000+ | ~108x |

The substrate will not be the bottleneck.

---

## What "Deterministic" Means Here

L0 does not claim the model is deterministic. Models are stochastic by nature. L0 claims the **execution substrate** is deterministic:

- The lifecycle order is specified and invariant.
- Events are normalized into a consistent shape.
- State tracking is consistent across providers.
- Recovery decisions are rule-driven and observable.
- Full executions can be recorded and replayed exactly from the event log.

This is the same kind of determinism a database transaction log provides: not that the world is predictable, but that the system's response to it is.

---

## Integration Surface

### Minimal Example

```python
import l0

result = await l0.run(
    lambda: client.chat.completions.create(
        model="gpt-4o", messages=messages, stream=True
    )
)
print(result.content)
```

### Client Wrapper

```python
import l0

# Wrap an OpenAI/LiteLLM client for automatic reliability
wrapped = l0.wrap(client, retry=l0.RECOMMENDED_RETRY)
response = await wrapped.chat.completions.create(
    model="gpt-4o", messages=messages, stream=True
)
```

### Full Configuration

```python
result = await l0.run(
    stream_factory,
    retry=l0.Retry(max_retries=3, strategy="exponential"),
    timeout=l0.Timeout(initial_token=10.0, inter_token=5.0),
    guardrails=l0.STRICT_GUARDRAILS,
    drift=True,
    continuation=l0.ContinuationConfig(enabled=True),
    fallbacks=[fallback_factory_1, fallback_factory_2],
    callbacks=l0.LifecycleCallbacks(
        on_violation=handle_violation,
        on_retry=log_retry,
    ),
)
```

---

## Testing

L0 is validated by 1,800+ unit tests and integration tests across 54 test files covering:

- All guardrail rules (JSON, Markdown, LaTeX, pattern, zero output) and fast/slow path execution.
- Drift detection (all seven drift types, sliding window behavior).
- Retry logic (all backoff strategies, error categories, budget tracking).
- Network error detection (all 15+ failure patterns).
- Structured output (Pydantic, JSON Schema, auto-correction).
- Consensus (all strategies and conflict resolution modes).
- Parallel, race, pipeline, and pool operations.
- Event sourcing (recording, replay, snapshots, storage backends, replay comparison).
- Adapters (OpenAI, LiteLLM, Anthropic, custom).
- Checkpoint resumption and continuation deduplication.
- Timeout enforcement (TTFT and inter-token).
- Canonical spec tests ensuring deterministic lifecycle parity with the TypeScript implementation.
- Performance benchmarks.

---

## Use Cases

- **Production chat**: consistent streaming semantics, timeouts, retries, fallbacks, and telemetry for user-facing applications.
- **Agent orchestration**: tool calls, partial failures, and multi-step reasoning with deterministic recovery at every step.
- **Structured extraction**: guaranteed-valid JSON with schema enforcement, auto-correction, and retry on validation failure.
- **Compliance and supervision**: guardrails, drift detection, and audit-ready replay logs for regulated industries.
- **Low-latency pipelines**: race for fastest-provider-wins, parallel for fan-out/fan-in, pipe for multi-stage streaming.
- **High-confidence generation**: multi-model consensus for safety-critical tasks where a single model's output is insufficient.
- **Multimodal applications**: image, audio, and video generation with the same reliability guarantees as text.
- **Long document processing**: document windowing with overlap for context-preserving chunking.

---

## Appendix A: Error Codes

| Code | Category | Description |
|---|---|---|
| `STREAM_ABORTED` | transient | Stream was aborted unexpectedly |
| `INITIAL_TOKEN_TIMEOUT` | transient | No first token within deadline |
| `INTER_TOKEN_TIMEOUT` | transient | Gap between tokens exceeded limit |
| `ZERO_OUTPUT` | content | Model produced empty/trivial output |
| `GUARDRAIL_VIOLATION` | content | Output violated a guardrail rule |
| `FATAL_GUARDRAIL_VIOLATION` | fatal | Output violated a fatal guardrail |
| `DRIFT_DETECTED` | content | Semantic drift detected |
| `INVALID_STREAM` | fatal | Stream is not a valid async iterable |
| `ADAPTER_NOT_FOUND` | fatal | No adapter could handle the stream |
| `ALL_STREAMS_EXHAUSTED` | fatal | All retries and fallbacks failed |
| `NETWORK_ERROR` | network | Transport-level failure |

## Appendix B: Drift Types

| Type | Detection method |
|---|---|
| `tone_shift` | Register/voice change analysis |
| `meta_commentary` | AI self-reference pattern matching |
| `format_collapse` | Structural degradation detection |
| `markdown_collapse` | Markdown formatting breakdown |
| `repetition` | Phrase/sentence loop detection |
| `entropy_spike` | Statistical surprise in token distribution |
| `hedging` | Excessive qualification language |

## Appendix C: Guardrail Severity Behavior

| Severity | Behavior |
|---|---|
| `warning` | Violation recorded; execution continues |
| `error` | Triggers retry if budget remains; otherwise recorded |
| `fatal` | Execution halts immediately |

## Appendix D: Event Sourcing Event Types

`START` · `TOKEN` · `CHECKPOINT` · `GUARDRAIL` · `DRIFT` · `RETRY` · `FALLBACK` · `CONTINUATION` · `COMPLETE` · `ERROR`

Each event is timestamped and carries a type-specific payload sufficient for exact replay.

## Appendix E: Feature Opt-In Model

Heavy features use explicit enablement:

- Drift detection: `drift=True`
- Checkpoint continuation: `continuation=ContinuationConfig(enabled=True)`
- Monitoring: `monitoring=MonitoringConfig(enabled=True)`
- Structured auto-correction: `auto_correct=True`

This ensures unused features incur no overhead.
