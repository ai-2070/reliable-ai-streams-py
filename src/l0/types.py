"""L0 types - clean Pythonic naming without module prefixes."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, Any, Coroutine, Generic, TypeVar

if TYPE_CHECKING:
    from .adapters import Adapter
    from .errors import NetworkError
    from .events import ObservabilityEvent
    from .guardrails import GuardrailRule, GuardrailViolation

# ─────────────────────────────────────────────────────────────────────────────
# Type Aliases for Stream Factories
# ─────────────────────────────────────────────────────────────────────────────

# TypeVar for LLM provider chunk types (OpenAI, LiteLLM, etc.)
# Covariant because streams produce chunks (output position only)
ChunkT = TypeVar("ChunkT", covariant=True)

# Invariant version for contexts that need both read and write
ChunkT_co = TypeVar("ChunkT_co")

# Raw stream from LLM provider (before adapter conversion)
RawStream = AsyncIterator[Any]

# Raw stream or coroutine that resolves to a stream (for unawaited async calls)
AwaitableStream = AsyncIterator[Any] | Coroutine[Any, Any, AsyncIterator[Any]]

# Generic raw stream with specific chunk type
RawStreamOf = AsyncIterator[ChunkT_co]

# Factory that creates a raw stream (for retry support)
StreamFactory = Callable[[], RawStream]

# Factory that can return either a stream or a coroutine resolving to a stream
AwaitableStreamFactory = Callable[[], AwaitableStream]

# Generic factory with specific chunk type
StreamFactoryOf = Callable[[], "AsyncIterator[ChunkT_co]"]

# Stream or factory (accepted by structured() and other APIs)
StreamSource = RawStream | StreamFactory

# Stream source that also accepts coroutines (for OpenAI-style unawaited calls)
AwaitableStreamSource = AwaitableStream | AwaitableStreamFactory


# ─────────────────────────────────────────────────────────────────────────────
# Event Types
# ─────────────────────────────────────────────────────────────────────────────


class EventType(str, Enum):
    """Type of streaming event."""

    TOKEN = "token"
    MESSAGE = "message"
    DATA = "data"
    PROGRESS = "progress"
    TOOL_CALL = "tool_call"
    ERROR = "error"
    COMPLETE = "complete"


class ContentType(str, Enum):
    """Type of multimodal content."""

    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    FILE = "file"
    JSON = "json"
    BINARY = "binary"


@dataclass
class DataPayload:
    """Multimodal data payload.

    Carries image, audio, video, file, or structured data from
    multimodal AI outputs.

    Attributes:
        content_type: Type of content (image, audio, video, file, json, binary)
        mime_type: MIME type (e.g., "image/png", "audio/mp3")
        base64: Base64-encoded data
        url: URL to content
        data: Raw bytes
        json: Structured JSON data
        metadata: Additional metadata (dimensions, duration, etc.)
    """

    content_type: ContentType
    mime_type: str | None = None
    base64: str | None = None
    url: str | None = None
    data: bytes | None = None
    json: Any | None = None
    metadata: dict[str, Any] | None = None

    # Convenience properties for common metadata
    @property
    def width(self) -> int | None:
        """Image/video width."""
        return self.metadata.get("width") if self.metadata else None

    @property
    def height(self) -> int | None:
        """Image/video height."""
        return self.metadata.get("height") if self.metadata else None

    @property
    def duration(self) -> float | None:
        """Audio/video duration in seconds."""
        return self.metadata.get("duration") if self.metadata else None

    @property
    def size(self) -> int | None:
        """File size in bytes."""
        return self.metadata.get("size") if self.metadata else None

    @property
    def filename(self) -> str | None:
        """Filename if available."""
        return self.metadata.get("filename") if self.metadata else None

    @property
    def seed(self) -> int | None:
        """Generation seed for reproducibility."""
        return self.metadata.get("seed") if self.metadata else None

    @property
    def model(self) -> str | None:
        """Model used for generation."""
        return self.metadata.get("model") if self.metadata else None


@dataclass
class Progress:
    """Progress update for long-running operations.

    Attributes:
        percent: Progress percentage (0-100)
        step: Current step number
        total_steps: Total number of steps
        message: Status message
        eta: Estimated time remaining in seconds
    """

    percent: float | None = None
    step: int | None = None
    total_steps: int | None = None
    message: str | None = None
    eta: float | None = None


@dataclass
class Event:
    """Unified event from adapter-normalized LLM stream.

    Usage:
        async for event in result:
            if event.is_token:
                print(event.text, end="")
            elif event.is_data:
                if event.payload.content_type == ContentType.IMAGE:
                    save_image(event.payload.base64)
            elif event.is_progress:
                print(f"Progress: {event.progress.percent}%")
            elif event.is_complete:
                print(f"Done! Usage: {event.usage}")
            elif event.is_error:
                print(f"Error: {event.error}")
    """

    type: EventType
    text: str | None = None  # Token content
    data: dict[str, Any] | None = None  # Tool call / misc data
    payload: DataPayload | None = None  # Multimodal data payload
    progress: Progress | None = None  # Progress update
    error: Exception | None = None  # Error (for error events)
    usage: dict[str, int] | None = None  # Token usage
    timestamp: float | None = None  # Event timestamp

    # ─────────────────────────────────────────────────────────────────────────
    # Type check helpers - beautiful Pythonic API
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def is_token(self) -> bool:
        """Check if this is a token event."""
        return self.type is EventType.TOKEN

    @property
    def is_message(self) -> bool:
        """Check if this is a message event."""
        return self.type is EventType.MESSAGE

    @property
    def is_data(self) -> bool:
        """Check if this is a data event."""
        return self.type is EventType.DATA

    @property
    def is_progress(self) -> bool:
        """Check if this is a progress event."""
        return self.type is EventType.PROGRESS

    @property
    def is_tool_call(self) -> bool:
        """Check if this is a tool call event."""
        return self.type is EventType.TOOL_CALL

    @property
    def is_error(self) -> bool:
        """Check if this is an error event."""
        return self.type is EventType.ERROR

    @property
    def is_complete(self) -> bool:
        """Check if this is a complete event."""
        return self.type is EventType.COMPLETE


# ─────────────────────────────────────────────────────────────────────────────
# Error Categories
# ─────────────────────────────────────────────────────────────────────────────


class ErrorCategory(str, Enum):
    """Category of error for retry decisions."""

    NETWORK = "network"
    TRANSIENT = "transient"
    MODEL = "model"
    CONTENT = "content"
    PROVIDER = "provider"
    FATAL = "fatal"
    INTERNAL = "internal"


class BackoffStrategy(str, Enum):
    """Backoff strategy for retries."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"
    FULL_JITTER = "full-jitter"
    FIXED_JITTER = "fixed-jitter"


# ─────────────────────────────────────────────────────────────────────────────
# State
# ─────────────────────────────────────────────────────────────────────────────


class _ContentDescriptor:
    """Descriptor that auto-flushes _content_buffer on read for O(n) total concat."""

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr = "_content_value"

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        buf = obj.__dict__.get("_content_buffer")
        if buf:
            obj.__dict__[self._attr] = obj.__dict__.get(self._attr, "") + "".join(buf)
            buf.clear()
        return obj.__dict__.get(self._attr, "")

    def __set__(self, obj: Any, value: str) -> None:
        obj.__dict__[self._attr] = value
        buf = obj.__dict__.get("_content_buffer")
        if buf:
            buf.clear()


@dataclass
class State:
    """Runtime state tracking."""

    content: str = ""  # type: ignore[assignment]  # managed by _ContentDescriptor
    _content_buffer: list[str] = field(default_factory=list, init=False, repr=False, compare=False)
    checkpoint: str = ""  # Last known good slice for continuation
    token_count: int = 0
    model_retry_count: int = 0
    network_retry_count: int = 0
    fallback_index: int = 0
    violations: "list[GuardrailViolation]" = field(default_factory=list)
    drift_detected: bool = False
    completed: bool = False
    aborted: bool = False
    first_token_at: float | None = None
    last_token_at: float | None = None
    duration: float | None = None
    resumed: bool = False  # Whether stream was resumed from checkpoint
    network_errors: "list[NetworkError]" = field(default_factory=list)
    # Multimodal state
    data_outputs: list[DataPayload] = field(default_factory=list)
    last_progress: Progress | None = None
    # Continuation state (for observability)
    resume_point: str | None = None  # The checkpoint content used for resume
    resume_from: int | None = None  # Character offset where resume occurred
    continuation_used: bool = False  # Whether continuation was actually used
    deduplication_applied: bool = False  # Whether deduplication removed overlap
    overlap_removed: str | None = None  # The overlapping text that was removed


# Install the descriptor after dataclass creation so it intercepts attribute access
State.content = _ContentDescriptor()  # type: ignore[assignment]
State.content.__set_name__(State, "content")


# ─────────────────────────────────────────────────────────────────────────────
# Retry + Timeout
# Retry delays are in seconds (Pythonic). Timeout uses milliseconds (TS parity).
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ErrorTypeDelays:
    """Per-error-type delay configuration.

    All delays are in seconds (float), matching Python conventions.
    Default values are sourced from ERROR_TYPE_DELAY_DEFAULTS for consistency.

    Usage:
        from l0 import ErrorTypeDelays, ERROR_TYPE_DELAY_DEFAULTS

        # Use all defaults
        delays = ErrorTypeDelays()

        # Override specific delays
        delays = ErrorTypeDelays(timeout=3.0, dns_error=5.0)

        # Check default values
        print(ERROR_TYPE_DELAY_DEFAULTS.timeout)  # 1.0
    """

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

    def __post_init__(self) -> None:
        """Apply defaults from ERROR_TYPE_DELAY_DEFAULTS for any unset values."""
        # Import here to avoid circular dependency at module load time
        # ERROR_TYPE_DELAY_DEFAULTS is defined after this class
        from . import types as _types

        defaults = _types.ERROR_TYPE_DELAY_DEFAULTS
        if self.connection_dropped is None:
            object.__setattr__(self, "connection_dropped", defaults.connection_dropped)
        if self.fetch_error is None:
            object.__setattr__(self, "fetch_error", defaults.fetch_error)
        if self.econnreset is None:
            object.__setattr__(self, "econnreset", defaults.econnreset)
        if self.econnrefused is None:
            object.__setattr__(self, "econnrefused", defaults.econnrefused)
        if self.sse_aborted is None:
            object.__setattr__(self, "sse_aborted", defaults.sse_aborted)
        if self.no_bytes is None:
            object.__setattr__(self, "no_bytes", defaults.no_bytes)
        if self.partial_chunks is None:
            object.__setattr__(self, "partial_chunks", defaults.partial_chunks)
        if self.runtime_killed is None:
            object.__setattr__(self, "runtime_killed", defaults.runtime_killed)
        if self.background_throttle is None:
            object.__setattr__(
                self, "background_throttle", defaults.background_throttle
            )
        if self.dns_error is None:
            object.__setattr__(self, "dns_error", defaults.dns_error)
        if self.ssl_error is None:
            object.__setattr__(self, "ssl_error", defaults.ssl_error)
        if self.timeout is None:
            object.__setattr__(self, "timeout", defaults.timeout)
        if self.unknown is None:
            object.__setattr__(self, "unknown", defaults.unknown)


class RetryableErrorType(str, Enum):
    """Error types that can be retried."""

    ZERO_OUTPUT = "zero_output"
    GUARDRAIL_VIOLATION = "guardrail_violation"
    DRIFT = "drift"
    INCOMPLETE = "incomplete"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"


# Default retryable error types
DEFAULT_RETRY_ON: list[RetryableErrorType] = [
    RetryableErrorType.ZERO_OUTPUT,
    RetryableErrorType.GUARDRAIL_VIOLATION,
    RetryableErrorType.DRIFT,
    RetryableErrorType.INCOMPLETE,
    RetryableErrorType.NETWORK_ERROR,
    RetryableErrorType.TIMEOUT,
    RetryableErrorType.RATE_LIMIT,
    RetryableErrorType.SERVER_ERROR,
]


# ─────────────────────────────────────────────────────────────────────────────
# Centralized Retry Defaults (matching TypeScript RETRY_DEFAULTS)
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RetryDefaults:
    """Centralized retry configuration defaults.

    All retry-related code should import these constants instead of hardcoding values.
    All delays are in seconds (float), matching Python conventions.

    Usage:
        from l0 import RETRY_DEFAULTS

        # Access defaults
        max_attempts = RETRY_DEFAULTS.attempts
        base_delay = RETRY_DEFAULTS.base_delay
        strategy = RETRY_DEFAULTS.backoff
    """

    attempts: int = 3
    """Maximum retry attempts for model failures."""

    max_retries: int = 6
    """Absolute maximum retries across all error types."""

    base_delay: float = 1.0
    """Base delay in seconds."""

    max_delay: float = 10.0
    """Maximum delay cap in seconds."""

    network_max_delay: float = 30.0
    """Maximum delay for network error suggestions in seconds."""

    backoff: BackoffStrategy = BackoffStrategy.FIXED_JITTER
    """Default backoff strategy (AWS-style fixed jitter for predictable retry timing)."""

    retry_on: tuple[RetryableErrorType, ...] = (
        RetryableErrorType.ZERO_OUTPUT,
        RetryableErrorType.GUARDRAIL_VIOLATION,
        RetryableErrorType.DRIFT,
        RetryableErrorType.INCOMPLETE,
        RetryableErrorType.NETWORK_ERROR,
        RetryableErrorType.TIMEOUT,
        RetryableErrorType.RATE_LIMIT,
        RetryableErrorType.SERVER_ERROR,
    )
    """Default retry reasons (unknown errors are not retried by default)."""


# Singleton instance - use this in all retry-related code
RETRY_DEFAULTS = RetryDefaults()


@dataclass(frozen=True)
class ErrorTypeDelayDefaults:
    """Default error-type-specific delays for network errors.

    All delays are in seconds (float), matching Python conventions.

    Usage:
        from l0 import ERROR_TYPE_DELAY_DEFAULTS

        # Access defaults
        timeout_delay = ERROR_TYPE_DELAY_DEFAULTS.timeout
        dns_delay = ERROR_TYPE_DELAY_DEFAULTS.dns_error
    """

    connection_dropped: float = 1.0
    fetch_error: float = 0.5
    econnreset: float = 1.0
    econnrefused: float = 2.0
    sse_aborted: float = 0.5
    no_bytes: float = 0.5
    partial_chunks: float = 0.5
    runtime_killed: float = 2.0
    background_throttle: float = 5.0
    dns_error: float = 3.0
    ssl_error: float = 0.0  # SSL errors are config issues, don't retry
    timeout: float = 1.0
    unknown: float = 1.0


# Singleton instance - use this in all error-type delay code
ERROR_TYPE_DELAY_DEFAULTS = ErrorTypeDelayDefaults()


@dataclass
class Retry:
    """Retry configuration.

    All delays are in seconds (float), matching Python conventions
    like asyncio.sleep(), time.sleep(), etc.

    Default values are sourced from RETRY_DEFAULTS for consistency.

    Usage:
        from l0 import Retry, RetryableErrorType, RETRY_DEFAULTS
        from l0 import MINIMAL_RETRY, RECOMMENDED_RETRY, STRICT_RETRY, EXPONENTIAL_RETRY

        # Use preset constants (matches TypeScript API)
        retry = MINIMAL_RETRY
        retry = RECOMMENDED_RETRY
        retry = STRICT_RETRY
        retry = EXPONENTIAL_RETRY

        # Use class method presets
        retry = Retry.recommended()
        retry = Retry.minimal()
        retry = Retry.strict()
        retry = Retry.exponential()
        retry = Retry.mobile()
        retry = Retry.edge()

        # Or customize
        retry = Retry(
            attempts=5,
            base_delay=2.0,
            error_type_delays=ErrorTypeDelays(
                timeout=3.0,
                connection_dropped=2.0,
            ),
        )

        # Only retry on specific error types
        retry = Retry(
            attempts=3,
            retry_on=[
                RetryableErrorType.NETWORK_ERROR,
                RetryableErrorType.TIMEOUT,
            ],
        )

        # Custom retry veto callback
        async def should_retry(error, state, attempt, category):
            # Return False to skip retry
            return attempt < 3 and not is_auth_error(error)

        retry = Retry(
            attempts=3,
            should_retry=should_retry,
        )

        # Custom delay calculation
        def custom_delay(context):
            # Return delay in seconds
            return min(context.attempt * 2.0, 30.0)

        retry = Retry(
            attempts=3,
            calculate_delay=custom_delay,
        )
    """

    attempts: int | None = None  # Model errors only (default: RETRY_DEFAULTS.attempts)
    max_retries: int | None = None  # Absolute cap (default: RETRY_DEFAULTS.max_retries)
    base_delay: float | None = (
        None  # Starting delay in seconds (default: RETRY_DEFAULTS.base_delay)
    )
    max_delay: float | None = (
        None  # Maximum delay in seconds (default: RETRY_DEFAULTS.max_delay)
    )
    strategy: BackoffStrategy | None = (
        None  # Backoff strategy (default: RETRY_DEFAULTS.backoff)
    )
    error_type_delays: ErrorTypeDelays | None = None  # Per-error-type delays
    retry_on: list[RetryableErrorType] | None = None  # Which error types to retry
    should_retry: Callable[..., bool | Coroutine[Any, Any, bool]] | None = (
        None  # Veto callback (sync or async)
    )
    calculate_delay: Callable[..., float] | None = None  # Custom delay calculation

    def __post_init__(self) -> None:
        """Apply defaults from RETRY_DEFAULTS for any unset values."""
        if self.attempts is None:
            object.__setattr__(self, "attempts", RETRY_DEFAULTS.attempts)
        if self.max_retries is None:
            object.__setattr__(self, "max_retries", RETRY_DEFAULTS.max_retries)
        if self.base_delay is None:
            object.__setattr__(self, "base_delay", RETRY_DEFAULTS.base_delay)
        if self.max_delay is None:
            object.__setattr__(self, "max_delay", RETRY_DEFAULTS.max_delay)
        if self.strategy is None:
            object.__setattr__(self, "strategy", RETRY_DEFAULTS.backoff)

    @classmethod
    def minimal(cls) -> "Retry":
        """Get minimal retry configuration.

        Lightweight retry for simple use cases:
        - 2 model error retries
        - 4 max total retries
        - Linear backoff strategy

        Matches TypeScript's minimalRetry preset.

        Returns:
            Retry configuration with minimal overhead.
        """
        return cls(
            attempts=2,
            max_retries=4,
            base_delay=1.0,
            max_delay=10.0,
            strategy=BackoffStrategy.LINEAR,
            error_type_delays=ErrorTypeDelays(),
        )

    @classmethod
    def recommended(cls) -> "Retry":
        """Get recommended retry configuration.

        Handles all network errors automatically with sensible defaults:
        - 3 model error retries
        - 6 max total retries
        - Fixed-jitter backoff strategy
        - Per-error-type delays for network errors

        Matches TypeScript's recommendedRetry preset.

        Returns:
            Retry configuration optimized for most use cases.
        """
        return cls(
            attempts=3,
            max_retries=6,
            base_delay=1.0,
            max_delay=10.0,
            strategy=BackoffStrategy.FIXED_JITTER,
            error_type_delays=ErrorTypeDelays(),
        )

    @classmethod
    def strict(cls) -> "Retry":
        """Get strict retry configuration.

        More aggressive jitter for high-load scenarios:
        - 3 model error retries
        - 6 max total retries
        - Full-jitter backoff strategy (AWS-recommended for thundering herd)

        Matches TypeScript's strictRetry preset.

        Returns:
            Retry configuration with full jitter for better load distribution.
        """
        return cls(
            attempts=3,
            max_retries=6,
            base_delay=1.0,
            max_delay=10.0,
            strategy=BackoffStrategy.FULL_JITTER,
            error_type_delays=ErrorTypeDelays(),
        )

    @classmethod
    def exponential(cls) -> "Retry":
        """Get exponential retry configuration.

        More retries with exponential backoff:
        - 4 model error retries
        - 8 max total retries
        - Exponential backoff strategy

        Matches TypeScript's exponentialRetry preset.

        Returns:
            Retry configuration with exponential backoff for longer operations.
        """
        return cls(
            attempts=4,
            max_retries=8,
            base_delay=1.0,
            max_delay=10.0,
            strategy=BackoffStrategy.EXPONENTIAL,
            error_type_delays=ErrorTypeDelays(),
        )

    @classmethod
    def mobile(cls) -> "Retry":
        """Get retry configuration optimized for mobile environments.

        Higher delays for background throttling and connection issues.
        """
        return cls(
            attempts=3,
            max_retries=6,
            base_delay=1.0,
            max_delay=15.0,
            strategy=BackoffStrategy.FULL_JITTER,
            error_type_delays=ErrorTypeDelays(
                background_throttle=15.0,
                timeout=3.0,
                connection_dropped=2.5,
            ),
        )

    @classmethod
    def edge(cls) -> "Retry":
        """Get retry configuration optimized for edge runtimes.

        Shorter delays to stay within edge runtime limits.
        """
        return cls(
            attempts=3,
            max_retries=6,
            base_delay=0.5,
            max_delay=5.0,
            strategy=BackoffStrategy.FIXED_JITTER,
            error_type_delays=ErrorTypeDelays(
                runtime_killed=2.0,
                timeout=1.5,
            ),
        )


@dataclass
class Timeout:
    """Timeout configuration.

    All timeouts are in milliseconds (int), matching TypeScript l0.

    Examples:
        timeout=Timeout(initial_token=5000, inter_token=10000)  # 5s, 10s
    """

    initial_token: int = 5000  # Milliseconds to first token (default: 5s)
    inter_token: int = 10000  # Milliseconds between tokens (default: 10s)


@dataclass
class CheckIntervals:
    """Configuration for check frequencies during streaming.

    Controls how often guardrails, drift detection, and checkpoints
    are evaluated during streaming. Lower values = more frequent checks
    (more CPU, better responsiveness). Higher values = less frequent
    (better performance for long outputs).

    Usage:
        from l0 import CheckIntervals

        # Default intervals
        intervals = CheckIntervals()

        # More frequent checks (for short, critical outputs)
        intervals = CheckIntervals(guardrails=2, drift=5, checkpoint=5)

        # Less frequent checks (for long outputs, better performance)
        intervals = CheckIntervals(guardrails=50, drift=100, checkpoint=50)
    """

    guardrails: int = (
        15  # Check guardrails every N tokens (optimized for high throughput)
    )
    drift: int = 25  # Check drift every N tokens (optimized for high throughput)
    checkpoint: int = 20  # Save checkpoint every N tokens


# ─────────────────────────────────────────────────────────────────────────────
# Stream (the result type)
# ─────────────────────────────────────────────────────────────────────────────

# TypeVar for Stream's chunk type (invariant for class definition)
_StreamChunkT = TypeVar("_StreamChunkT")


class Stream(Generic[_StreamChunkT]):
    """Async iterator result with state and abort attached.

    Generic over the raw chunk type from the LLM provider (e.g., OpenAI's
    ChatCompletionChunk). Use `raw()` to access raw chunks for provider-specific
    processing.

    Supports both iteration and context manager patterns:

        # Pattern 1: Direct iteration (l0.wrap - no await needed!)
        result = l0.wrap(stream)
        async for event in result:
            if event.is_token:
                print(event.text, end="")

        # Pattern 2: Context manager (auto-cleanup)
        async with l0.wrap(stream) as result:
            async for event in result:
                if event.is_token:
                    print(event.text, end="")

        # Get full text
        text = await result.read()

        # Access raw chunks (provider-specific)
        for chunk in result.raw():
            print(chunk)  # OpenAI ChatCompletionChunk, etc.

        # Access state (after iteration)
        print(result.state.content)
        print(result.state.token_count)
    """

    __slots__ = (
        "_iterator",
        "_consumed",
        "_content",
        "_raw_chunks",
        "state",
        "abort",
        "errors",
    )

    def __init__(
        self,
        iterator: AsyncIterator[Event],
        state: State,
        abort: Callable[[], None],
        errors: list[Exception] | None = None,
        raw_chunks: list[_StreamChunkT] | None = None,
    ) -> None:
        self._iterator = iterator
        self._consumed = False
        self._content: str | None = None
        self._raw_chunks: list[_StreamChunkT] = (
            raw_chunks if raw_chunks is not None else []
        )
        self.state = state
        self.abort = abort
        self.errors = errors or []

    # ─────────────────────────────────────────────────────────────────────────
    # Async iterator protocol
    # ─────────────────────────────────────────────────────────────────────────

    def __aiter__(self) -> "Stream[_StreamChunkT]":
        return self

    async def __anext__(self) -> Event:
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            self._consumed = True
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Context manager protocol
    # ─────────────────────────────────────────────────────────────────────────

    async def __aenter__(self) -> "Stream[_StreamChunkT]":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.abort()
        return False  # Don't suppress exceptions

    # ─────────────────────────────────────────────────────────────────────────
    # Read interface
    # ─────────────────────────────────────────────────────────────────────────

    async def read(self) -> str:
        """Consume the stream and return the full text content.

        Pythonic interface matching file.read(), stream.read(), etc.
        If already consumed, returns the accumulated state.content.
        """
        if self._consumed or self._content is not None:
            return self._content or self.state.content

        # Consume the stream
        async for _ in self:
            pass  # Events are processed, state is updated

        self._content = self.state.content
        return self._content

    # ─────────────────────────────────────────────────────────────────────────
    # Raw chunks interface
    # ─────────────────────────────────────────────────────────────────────────

    def raw(self) -> list[_StreamChunkT]:
        """Get the raw chunks from the LLM provider.

        Returns the original chunk objects (e.g., OpenAI's ChatCompletionChunk)
        that were received during streaming. Only available after the stream
        has been consumed.

        Returns:
            List of raw chunks in order received

        Example:
            ```python
            result = await l0.run(stream_factory)
            text = await result.read()

            # Access raw OpenAI chunks
            for chunk in result.raw():
                if chunk.usage:
                    print(f"Tokens: {chunk.usage.total_tokens}")
            ```
        """
        return self._raw_chunks

    def _append_raw_chunk(self, chunk: _StreamChunkT) -> None:
        """Internal: Append a raw chunk during streaming."""
        self._raw_chunks.append(chunk)


# TypeVar for LazyStream's chunk type (invariant for class definition)
_LazyStreamChunkT = TypeVar("_LazyStreamChunkT")


class LazyStream(Generic[_LazyStreamChunkT]):
    """Lazy stream wrapper - no await needed on creation.

    Generic over the raw chunk type from the LLM provider (e.g., OpenAI's
    ChatCompletionChunk). Use `raw()` to access raw chunks for provider-specific
    processing.

    Like httpx.AsyncClient() or aiohttp.ClientSession(), this returns
    immediately and only does async work when you iterate or read.

    Usage:
        # Simple - no double await!
        result = l0.wrap(stream)
        text = await result.read()

        # Streaming
        async for event in l0.wrap(stream):
            print(event.text)

        # Context manager
        async with l0.wrap(stream) as result:
            async for event in result:
                print(event.text)

        # Access raw chunks
        for chunk in result.raw():
            print(chunk)
    """

    __slots__ = (
        "_stream",
        "_guardrails",
        "_timeout",
        "_adapter",
        "_on_event",
        "_on_token",
        "_on_tool_call",
        "_on_violation",
        "_context",
        "_buffer_tool_calls",
        "_runner",
        "_started",
    )

    def __init__(
        self,
        stream: AsyncIterator[_LazyStreamChunkT],
        *,
        guardrails: "list[GuardrailRule] | None" = None,
        timeout: Timeout | None = None,
        adapter: "Adapter | str | None" = None,
        on_event: Callable[[ObservabilityEvent], None] | None = None,
        on_token: Callable[[str], None] | None = None,
        on_tool_call: Callable[[str, str, dict[str, Any]], None] | None = None,
        on_violation: "Callable[[GuardrailViolation], None] | None" = None,
        context: dict[str, Any] | None = None,
        buffer_tool_calls: bool = False,
    ) -> None:
        self._stream = stream
        self._guardrails = guardrails
        self._timeout = timeout
        self._adapter = adapter
        self._on_event = on_event
        self._on_token = on_token
        self._on_tool_call = on_tool_call
        self._on_violation = on_violation
        self._context = context
        self._buffer_tool_calls = buffer_tool_calls
        self._runner: Stream[_LazyStreamChunkT] | None = None
        self._started = False

    async def _ensure_started(self) -> "Stream[_LazyStreamChunkT]":
        """Lazily start the L0 runtime."""
        if self._runner is None:
            # Import here to avoid circular import
            from .runtime import _internal_run

            # Wrap stream in factory
            stream = self._stream

            def stream_factory() -> AsyncIterator[_LazyStreamChunkT]:
                return stream

            self._runner = await _internal_run(
                stream=stream_factory,
                fallbacks=None,
                guardrails=self._guardrails,
                retry=None,  # Retry not supported - stream can't be recreated
                timeout=self._timeout,
                adapter=self._adapter,
                on_event=self._on_event,
                on_token=self._on_token,
                on_tool_call=self._on_tool_call,
                on_violation=self._on_violation,
                context=self._context,
                buffer_tool_calls=self._buffer_tool_calls,
            )
            self._started = True
        return self._runner

    # ─────────────────────────────────────────────────────────────────────────
    # Proxy properties (delegate to runner once started)
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def state(self) -> State:
        """Get state (only valid after iteration starts)."""
        if self._runner is None:
            # Return empty state before started
            return State()
        return self._runner.state

    @property
    def errors(self) -> list[Exception]:
        """Get errors list."""
        if self._runner is None:
            return []
        return self._runner.errors

    def abort(self) -> None:
        """Abort the stream."""
        if self._runner is not None:
            self._runner.abort()

    # ─────────────────────────────────────────────────────────────────────────
    # Async iterator protocol
    # ─────────────────────────────────────────────────────────────────────────

    def __aiter__(self) -> "LazyStream[_LazyStreamChunkT]":
        return self

    async def __anext__(self) -> Event:
        runner = await self._ensure_started()
        return await runner.__anext__()

    # ─────────────────────────────────────────────────────────────────────────
    # Context manager protocol
    # ─────────────────────────────────────────────────────────────────────────

    async def __aenter__(self) -> "LazyStream[_LazyStreamChunkT]":
        await self._ensure_started()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.abort()
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Read interface
    # ─────────────────────────────────────────────────────────────────────────

    async def read(self) -> str:
        """Consume the stream and return the full text content."""
        runner = await self._ensure_started()
        return await runner.read()

    # ─────────────────────────────────────────────────────────────────────────
    # Raw chunks interface
    # ─────────────────────────────────────────────────────────────────────────

    def raw(self) -> list[_LazyStreamChunkT]:
        """Get the raw chunks from the LLM provider.

        Returns the original chunk objects (e.g., OpenAI's ChatCompletionChunk)
        that were received during streaming. Only available after the stream
        has been consumed.

        Returns:
            List of raw chunks in order received
        """
        if self._runner is None:
            return []
        return self._runner.raw()


# ─────────────────────────────────────────────────────────────────────────────
# Retry Presets (TypeScript parity)
# ─────────────────────────────────────────────────────────────────────────────

# These match TypeScript's minimalRetry, recommendedRetry, strictRetry, exponentialRetry

MINIMAL_RETRY = Retry(
    attempts=2,
    max_retries=4,
    base_delay=1.0,
    max_delay=10.0,
    strategy=BackoffStrategy.LINEAR,
    error_type_delays=ErrorTypeDelays(),
)
"""Minimal retry preset: 2 attempts, 4 max retries, linear backoff."""

RECOMMENDED_RETRY = Retry(
    attempts=3,
    max_retries=6,
    base_delay=1.0,
    max_delay=10.0,
    strategy=BackoffStrategy.FIXED_JITTER,
    error_type_delays=ErrorTypeDelays(),
)
"""Recommended retry preset: 3 attempts, 6 max retries, fixed-jitter backoff."""

STRICT_RETRY = Retry(
    attempts=3,
    max_retries=6,
    base_delay=1.0,
    max_delay=10.0,
    strategy=BackoffStrategy.FULL_JITTER,
    error_type_delays=ErrorTypeDelays(),
)
"""Strict retry preset: 3 attempts, 6 max retries, full-jitter backoff."""

EXPONENTIAL_RETRY = Retry(
    attempts=4,
    max_retries=8,
    base_delay=1.0,
    max_delay=10.0,
    strategy=BackoffStrategy.EXPONENTIAL,
    error_type_delays=ErrorTypeDelays(),
)
"""Exponential retry preset: 4 attempts, 8 max retries, exponential backoff."""
