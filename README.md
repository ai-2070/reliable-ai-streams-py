# L0 - Deterministic Streaming Execution Substrate (DSES) for AI

### The missing reliability and observability layer for all AI streams.

![L0: The Missing AI Reliability Substrate](img/l0-banner.jpg)

<p align="center">
  <a href="https://pypi.org/project/ai2070-l0/">
    <img src="https://img.shields.io/pypi/v/ai2070-l0?color=brightgreen&label=pypi" alt="PyPI version">
  </a>
  <a href="https://pypi.org/project/ai2070-l0/">
    <img src="https://img.shields.io/pypi/pyversions/ai2070-l0" alt="Python versions">
  </a>
  <img src="https://img.shields.io/badge/types-included-blue?logo=python&logoColor=white" alt="Types Included">
  <img src="https://img.shields.io/badge/asyncio-native-blue" alt="Asyncio Native">
  <img src="https://img.shields.io/badge/tests-1800+-blue" alt="1800+ Tests">
  <img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="Apache 2.0 License">
</p>

> LLMs produce high-value reasoning over a low-integrity transport layer.
> Streams stall, drop tokens, reorder events, violate timing guarantees, and expose no deterministic contract.
>
> This breaks retries. It breaks supervision. It breaks reproducibility.
> It makes reliable AI systems impossible to build on top of raw provider streams.
>
> **L0 is the deterministic execution substrate that fixes the transport - with guardrails designed specifically for the streaming layer: stream-neutral, pattern-based, loop-safe, and timing-aware.**  
>  
> **The result: production-grade, integrity-preserving, deterministic AI streams you can finally build real systems on.**

It works with **OpenAI** and **LiteLLM** (100+ providers including Anthropic, Cohere, Bedrock, Vertex, Gemini). Supports **tool calls** and provides full observability.

```bash
pip install ai2070-l0
```

**Also available in TypeScript:** [@ai-2070/l0](https://github.com/ai-2070/l0) `npm install @ai2070/l0` - native implementation with full lifecycle and event signature parity.

_Production-grade reliability. Just pass your stream. L0'll take it from here._

L0 includes 1,800+ tests covering all major reliability features.

```
   Any AI Stream                    L0 Layer                         Your App
 ─────────────────    ┌──────────────────────────────────────┐    ─────────────
                      │                                      │
   OpenAI / LiteLLM   │   Retry · Fallback · Resume          │      Reliable
   Custom Streams  ──▶│   Guardrails · Timeouts · Consensus  │─────▶ Output
                      │   Full Observability                 │
                      │                                      │
                      └──────────────────────────────────────┘
 ─────────────────                                                ─────────────
                           L0 = Token-Level Reliability
```

## Features

| Feature                                        | Description                                                                                                                                                                                           |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **🔁 Smart Retries**                           | Model-aware retries with fixed-jitter backoff. Automatic retries for zero-token output, network stalls, and provider overloads.                                                                      |
| **🌐 Network Protection**                      | Automatic recovery from dropped streams, slow responses, 429/503 load shedding, DNS errors, and partial chunks.                                                                                       |
| **🔀 Model Fallbacks**                         | Automatically fallback to secondary models (e.g., GPT-4o → GPT-4o-mini → Claude) with full retry logic.                                                                                               |
| **💥 Zero-Token/Stall Protection**             | Detects when model produces nothing or stalls mid-stream. Automatically retries or switches to fallbacks.                                                                                             |
| **📍 Last-Known-Good Token Resumption**        | `continue_from_last_good_token` resumes from the last checkpoint on timeout or failure. No lost tokens.                                                                                              |
| **🧠 Drift Detection**                         | Detects repetition, stalls, and format drift before corruption propagates.                                                                                                                            |
| **🧱 Structured Output**                       | Guaranteed-valid JSON with Pydantic. Auto-corrects missing braces, commas, and markdown fences.                                                                                                       |
| **📋 Pydantic Validation Models**              | All L0 types available as Pydantic models for runtime validation, JSON serialization, and schema generation via `l0.pydantic`.                                                                       |
| **🩹 JSON Auto-Healing**                       | Automatic correction of truncated or malformed JSON (missing braces, brackets, quotes), and repair of broken Markdown code fences.                                                                   |
| **🛡️ Guardrails**                              | JSON, Markdown, and pattern validation with fast streaming checks. Delta-only checks run sync; full-content scans defer to async.                                                                    |
| **⚡ Race: Fastest-Model Wins**                | Run multiple models or providers in parallel and return the fastest valid stream. Ideal for ultra-low-latency chat.                                                                                  |
| **🌿 Parallel: Fan-Out / Fan-In**              | Start multiple streams simultaneously and collect structured or summarized results. Perfect for agent-style multi-model workflows.                                                                    |
| **🧩 Consensus: Agreement Across Models**      | Combine multiple model outputs using unanimous, majority, or best-match consensus. Guarantees high-confidence generation.                                                                             |
| **🔔 Lifecycle Callbacks**                     | `on_start`, `on_complete`, `on_error`, `on_event`, `on_violation`, `on_retry`, `on_fallback`, `on_tool_call` - full observability into every stream phase.                                           |
| **📡 Streaming-First Runtime**                 | Thin, deterministic wrapper with unified event types (`token`, `error`, `complete`) for easy UIs.                                                                                                     |
| **📼 Central Event Bus**                       | Full observability into every stream phase via `on_event` callback with 25+ structured event types.                                                                                                   |
| **🔌 Custom Adapters (BYOA)**                  | Bring your own adapter for any LLM provider. Built-in adapters for OpenAI and LiteLLM.                                                                                                                |
| **📦 Raw Chunk Access**                        | Access original provider chunks (e.g., OpenAI's `ChatCompletionChunk`) via `stream.raw()` for provider-specific processing.                                                                          |
| **⚡ Pure asyncio**                            | No compatibility layers (no anyio/trio). Native Python async for full determinism and performance.                                                                                                    |
| **🔧 Own Retry Logic**                         | No external dependencies (no tenacity). L0 controls all retry behavior for predictable execution.                                                                                                     |
| **📝 Type-Safe**                               | Full type hints with `py.typed` marker. Passes mypy strict mode.                                                                                                                                      |
| **📦 Minimal Dependencies**                    | Only httpx, pydantic, orjson, typing-extensions, uuid6. No heavy abstractions.                                                                                                                        |
| **🚀 Nvidia Blackwell-Ready**                    | Optimized for 1000+ tokens/s streaming. Ready for next-gen GPU inference speeds.                                                                                                                      |
| **🧪 Battle-Tested**                           | 1,800+ unit tests and 100+ integration tests validating real streaming, retries, and advanced behavior.                                                                                               |

> **Know what you're doing?** [Skip the tutorial](./ADVANCED.md)

## Quick Start

### Wrap Your Client (Recommended)

```python
import asyncio
from openai import AsyncOpenAI
import l0

async def main():
    # Wrap the client once - L0 reliability is automatic
    client = l0.wrap(AsyncOpenAI())

    # Use normally - no lambdas needed!
    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": "Hello!"}],
        stream=True,
    )

    # Stream with L0 events
    async for event in response:
        if event.is_token:
            print(event.text, end="", flush=True)

    # Or read all at once
    text = await response.read()

asyncio.run(main())
```

### With Configuration

```python
import l0
from openai import AsyncOpenAI

# Configure once, use everywhere
client = l0.wrap(
    AsyncOpenAI(),
    guardrails=l0.Guardrails.recommended(),
    retry=l0.Retry(attempts=5),
    timeout=l0.Timeout(initial_token=10000, inter_token=30000),
    continue_from_last_good_token=True,  # Resume from checkpoint on failure
)

response = await client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True,
)
```

**See Also: [API.md](./API.md) for all options, [ADVANCED.md](./ADVANCED.md) for full examples**

### With LiteLLM (100+ Providers)

```python
import asyncio
import litellm
import l0

async def main():
    # For LiteLLM, use l0.run() with a factory function
    result = await l0.run(
        stream=lambda: litellm.acompletion(
            model="anthropic/claude-3-haiku-20240307",
            messages=[{"role": "user", "content": "Hello!"}],
            stream=True,
        ),
        guardrails=l0.Guardrails.recommended(),
    )

    # Read full text
    text = await result.read()
    print(text)

asyncio.run(main())
```

### Structured Output with Pydantic

```python
from pydantic import BaseModel
import l0

class UserProfile(BaseModel):
    name: str
    age: int
    occupation: str

result = await l0.structured(
    schema=UserProfile,
    stream=lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": "Generate a fictional person as JSON"}],
        stream=True,
    ),
    auto_correct=True,  # Fix trailing commas, missing braces, markdown fences
)

print(result.name)  # "Alice"
print(result.age)   # 32
```

### Lifecycle Events

```python
import l0

result = await l0.run(
    stream=lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        stream=True,
    ),
    on_stream_event=lambda event: (
        print(event.text, end="") if event.is_token else
        print(f"\nError: {event.error}") if event.is_error else
        print("\nDone!") if event.is_complete else None
    ),
)
```

### Fallback Models & Providers

```python
import l0

result = await l0.run(
    # Primary model
    stream=lambda: client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        stream=True,
    ),
    # Fallbacks: tried in order if primary fails
    fallbacks=[
        lambda: client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        ),
        lambda: litellm.acompletion(
            model="anthropic/claude-3-haiku-20240307",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        ),
    ],
    on_fallback=lambda index, reason: print(f"Switched to fallback {index}"),
)
```

### Parallel Execution

```python
import l0

prompts = ["Name a fruit", "Name a color", "Name an animal"]

result = await l0.parallel(
    tasks=[
        lambda p=p: l0.run(
            stream=lambda: client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": p}],
                stream=True,
            ),
        )
        for p in prompts
    ],
    concurrency=3,
)

for prompt, stream in zip(prompts, result.results):
    text = await stream.read()
    print(f"{prompt}: {text.strip()}")
```

### Pydantic Validation Models

L0 provides Pydantic models for all its types, enabling runtime validation, JSON serialization, and schema generation:

```python
from l0.pydantic import StateModel, RetryModel, DriftResultModel

# Validate external data
state = StateModel(content="Hello", token_count=5, completed=True)

# Serialize to JSON
json_data = state.model_dump_json()

# Generate JSON schema for documentation or APIs
schema = StateModel.model_json_schema()
```

All L0 types have corresponding Pydantic models: `StateModel`, `RetryModel`, `TimeoutModel`, `ConsensusResultModel`, `DriftResultModel`, `MetricsSnapshotModel`, and more.

## Philosophy

- **No magic** - Everything is explicit and predictable
- **Streaming-first** - Built for real-time token delivery
- **Signals, not rewrites** - Guardrails detect issues, don't modify output
- **Model-agnostic** - Works with any provider via adapters
- **Pure asyncio** - No compatibility layers, native Python async
- **Own retry logic** - No tenacity, full control over behavior

## Documentation

| Guide                                            | Description                              |
| ------------------------------------------------ | ---------------------------------------- |
| [QUICKSTART.md](./QUICKSTART.md)                 | Get started in 5 minutes                 |
| [ADVANCED.md](./ADVANCED.md)                     | Advanced usage and full examples         |
| [API.md](./API.md)                               | Complete API reference                   |
| [GUARDRAILS.md](./GUARDRAILS.md)                 | Guardrails and validation                |
| [STRUCTURED_OUTPUT.md](./STRUCTURED_OUTPUT.md)   | Structured output guide                  |
| [CONSENSUS.md](./CONSENSUS.md)                   | Multi-generation consensus               |
| [DETERMINISTIC_LIFECYCLE.md](./DETERMINISTIC_LIFECYCLE.md) | Lifecycle specification and events |
| [NETWORK_ERRORS.md](./NETWORK_ERRORS.md)         | Network error handling                   |
| [ERROR_HANDLING.md](./ERROR_HANDLING.md)         | Error handling guide                     |
| [CUSTOM_ADAPTERS.md](./CUSTOM_ADAPTERS.md)       | Build your own adapters                  |
| [DOCUMENT_WINDOWS.md](./DOCUMENT_WINDOWS.md)     | Chunking and processing long documents   |
| [EVENT_SOURCING.md](./EVENT_SOURCING.md)         | Record/replay, audit trails              |
| [MONITORING.md](./MONITORING.md)                 | OpenTelemetry and Sentry integrations    |
| [FORMATTING.md](./FORMATTING.md)                 | Context, memory, output, and tool formatting |
| [PARALLEL_OPERATIONS.md](./PARALLEL_OPERATIONS.md) | Parallel, race, batch, and pool operations |
| [MULTIMODAL.md](./MULTIMODAL.md)                 | Image, audio, video, and multimodal adapters |
| [PERFORMANCE.md](./PERFORMANCE.md)               | Performance tuning guide                 |

## Installation

```bash
# Basic installation
pip install ai2070-l0

# With OpenAI support
pip install ai2070-l0[openai]

# With LiteLLM (100+ providers)
pip install ai2070-l0[litellm]

# With OpenTelemetry
pip install ai2070-l0[otel]

# With Sentry
pip install ai2070-l0[sentry]

# Development (dev is a dependency-group, not a pip extra)
uv sync --group dev
```

Or with uv:

```bash
uv add ai2070-l0
uv add ai2070-l0 --extra openai
uv add ai2070-l0 --extra litellm
```

### Dependencies

| Package | Purpose |
| ------- | ------- |
| `httpx` | HTTP client |
| `pydantic` | Schema validation |
| `orjson` | Fast JSON |
| `uuid6` | UUIDv7 for stream IDs |
| `typing-extensions` | Type hints |

### Optional Dependencies

| Extra | Packages |
| ----- | -------- |
| `openai` | `openai>=2.0,<3.0` |
| `litellm` | `litellm>=1.40` |
| `otel` | `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-instrumentation-httpx` |
| `sentry` | `sentry-sdk` |
| `observability` | `otel` + `sentry` combined |
| `speed` | `uvloop` (Unix only) |
| `dev` | `pytest`, `pytest-asyncio`, `pytest-cov`, `mypy`, `ruff` |

## License

Apache-2.0
