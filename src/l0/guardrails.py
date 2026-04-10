# mypy: disable-error-code="valid-type"
"""L0 guardrails engine with built-in rules and drift detection."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from .logging import logger

if TYPE_CHECKING:
    from .types import State

Severity = Literal["warning", "error", "fatal"]


@dataclass
class GuardrailViolation:
    """Guardrail violation details."""

    rule: str  # Name of the rule that was violated
    message: str  # Human-readable message
    severity: Severity  # Severity of the violation
    recoverable: bool = True  # Whether this violation is recoverable via retry
    position: int | None = None  # Position in content where violation occurred
    timestamp: float | None = None  # Timestamp when violation was detected
    context: dict[str, Any] | None = None  # Additional context about the violation
    suggestion: str | None = None  # Suggested fix or action


@dataclass
class GuardrailContext:
    """Context passed to guardrail rules.

    This is the TypeScript-compatible context type. For backwards compatibility,
    rules can also accept State objects.
    """

    content: str  # Current accumulated content
    completed: bool = False  # Whether stream is complete
    checkpoint: str | None = None  # Previous checkpoint content
    delta: str | None = None  # Current token delta (latest chunk)
    token_count: int = 0  # Total tokens received
    metadata: dict[str, Any] | None = None  # Stream metadata
    previous_violations: list[GuardrailViolation] | None = None  # Previous violations


@dataclass
class GuardrailResultSummary:
    """Summary of guardrail check results."""

    total: int
    fatal: int
    errors: int
    warnings: int


@dataclass
class GuardrailResult:
    """Result from running guardrails."""

    passed: bool  # Whether all checks passed
    violations: list[GuardrailViolation]  # All violations found
    should_retry: bool  # Whether content should be retried
    should_halt: bool  # Whether execution should halt
    summary: GuardrailResultSummary  # Summary of results


@dataclass
class GuardrailState:
    """Guardrail engine state."""

    violations: list[GuardrailViolation] = field(default_factory=list)
    violations_by_rule: dict[str, list[GuardrailViolation]] = field(
        default_factory=dict
    )
    has_fatal_violations: bool = False
    has_error_violations: bool = False
    violation_count: int = 0
    last_check_time: float | None = None


@dataclass
class GuardrailConfig:
    """Guardrail engine configuration."""

    rules: list["GuardrailRule"] = field(default_factory=list)
    stop_on_fatal: bool = True  # Whether to stop on first fatal violation
    enable_streaming: bool = True  # Whether to run streaming checks
    check_interval: int = 100  # Interval for streaming checks (in tokens or ms)
    on_violation: Callable[[GuardrailViolation], None] | None = (
        None  # Callback when violation is detected
    )


@dataclass
class GuardrailRule:
    """Guardrail rule definition."""

    name: str  # Unique name of the rule
    check: Callable[[State], list[GuardrailViolation]]  # Check function
    description: str | None = None  # Description of what the rule checks
    streaming: bool = True  # Whether to run on every token or only at completion
    severity: Severity = "error"  # Default severity for violations from this rule
    recoverable: bool = True  # Whether violations are recoverable via retry


# ─────────────────────────────────────────────────────────────────────────────
# Guardrail Engine
# ─────────────────────────────────────────────────────────────────────────────


class GuardrailEngine:
    """Guardrail engine for executing rules and managing violations.

    Example:
        >>> engine = GuardrailEngine(GuardrailConfig(rules=[json_rule()]))
        >>> result = engine.check(GuardrailContext(content='{"key": 1}', completed=True))
        >>> result.passed
        True
    """

    def __init__(self, config: GuardrailConfig | None = None) -> None:
        if config is None:
            config = GuardrailConfig()
        self._rules = list(config.rules)
        self._config = config
        self._state = self._create_initial_state()

    def _create_initial_state(self) -> GuardrailState:
        """Create initial guardrail state."""
        return GuardrailState()

    def check(self, context: GuardrailContext | State) -> GuardrailResult:
        """Execute all rules against context.

        Args:
            context: GuardrailContext or State object.

        Returns:
            GuardrailResult with violations and flags.
        """
        # Convert GuardrailContext to State-like for rule compatibility
        if isinstance(context, GuardrailContext):
            from .types import State as StateType

            state = StateType(
                content=context.content,
                completed=context.completed,
                token_count=context.token_count,
            )
        else:
            state = context

        violations: list[GuardrailViolation] = []
        timestamp = time.time()

        # Execute each rule
        for rule in self._rules:
            # Skip streaming rules if not enabled or not streaming check
            if (
                rule.streaming
                and not self._config.enable_streaming
                and not state.completed
            ):
                continue

            # Skip non-streaming rules if streaming check
            if not rule.streaming and not state.completed:
                continue

            try:
                rule_violations = rule.check(state)

                # Add timestamp to violations
                for violation in rule_violations:
                    violation.timestamp = timestamp
                    violations.append(violation)

                # Track violations by rule
                if rule_violations:
                    existing = self._state.violations_by_rule.get(rule.name, [])
                    self._state.violations_by_rule[rule.name] = (
                        existing + rule_violations
                    )

                # Stop on fatal if configured
                if self._config.stop_on_fatal and any(
                    v.severity == "fatal" for v in rule_violations
                ):
                    break

            except Exception as e:
                # Rule execution failed - treat as warning
                violations.append(
                    GuardrailViolation(
                        rule=rule.name,
                        message=f"Rule execution failed: {e}",
                        severity="warning",
                        recoverable=True,
                        timestamp=timestamp,
                    )
                )

        # Update state
        self._state.violations.extend(violations)
        self._state.violation_count = len(self._state.violations)
        self._state.has_fatal_violations = any(
            v.severity == "fatal" for v in violations
        )
        self._state.has_error_violations = any(
            v.severity == "error" for v in violations
        )
        self._state.last_check_time = timestamp

        # Notify callback
        if self._config.on_violation:
            for violation in violations:
                self._config.on_violation(violation)

        # Build result
        return GuardrailResult(
            passed=len(violations) == 0,
            violations=violations,
            should_retry=self._should_retry(violations),
            should_halt=self._should_halt(violations),
            summary=GuardrailResultSummary(
                total=len(violations),
                fatal=sum(1 for v in violations if v.severity == "fatal"),
                errors=sum(1 for v in violations if v.severity == "error"),
                warnings=sum(1 for v in violations if v.severity == "warning"),
            ),
        )

    def _should_retry(self, violations: list[GuardrailViolation]) -> bool:
        """Determine if violations should trigger a retry."""
        return any(
            v.recoverable and v.severity in ("error", "fatal") for v in violations
        )

    def _should_halt(self, violations: list[GuardrailViolation]) -> bool:
        """Determine if violations should halt execution."""
        # Halt on fatal violations
        if any(v.severity == "fatal" for v in violations):
            return True
        # Halt on non-recoverable errors
        if any(not v.recoverable and v.severity == "error" for v in violations):
            return True
        return False

    def get_state(self) -> GuardrailState:
        """Get current state."""
        return GuardrailState(
            violations=list(self._state.violations),
            violations_by_rule=dict(self._state.violations_by_rule),
            has_fatal_violations=self._state.has_fatal_violations,
            has_error_violations=self._state.has_error_violations,
            violation_count=self._state.violation_count,
            last_check_time=self._state.last_check_time,
        )

    def reset(self) -> None:
        """Reset state."""
        self._state = self._create_initial_state()

    def add_rule(self, rule: GuardrailRule) -> None:
        """Add a rule to the engine."""
        self._rules.append(rule)

    def remove_rule(self, rule_name: str) -> bool:
        """Remove a rule from the engine by name."""
        for i, rule in enumerate(self._rules):
            if rule.name == rule_name:
                self._rules.pop(i)
                return True
        return False

    def get_violations_by_rule(self, rule_name: str) -> list[GuardrailViolation]:
        """Get violations for a specific rule."""
        return list(self._state.violations_by_rule.get(rule_name, []))

    def get_all_violations(self) -> list[GuardrailViolation]:
        """Get all violations."""
        return list(self._state.violations)

    def has_violations(self) -> bool:
        """Check if any violations exist."""
        return self._state.violation_count > 0

    def has_fatal_violations(self) -> bool:
        """Check if any fatal violations exist."""
        return self._state.has_fatal_violations

    def has_error_violations(self) -> bool:
        """Check if any error violations exist."""
        return self._state.has_error_violations


def create_guardrail_engine(
    rules: list[GuardrailRule],
    *,
    stop_on_fatal: bool = True,
    enable_streaming: bool = True,
    check_interval: int = 100,
    on_violation: Callable[[GuardrailViolation], None] | None = None,
) -> GuardrailEngine:
    """Create a guardrail engine with rules.

    Args:
        rules: List of guardrail rules.
        stop_on_fatal: Whether to stop on first fatal violation.
        enable_streaming: Whether to run streaming checks.
        check_interval: Interval for streaming checks.
        on_violation: Callback when violation is detected.

    Returns:
        Configured GuardrailEngine.

    Example:
        >>> engine = create_guardrail_engine([json_rule(), markdown_rule()])
        >>> result = engine.check(GuardrailContext(content='{}', completed=True))
    """
    return GuardrailEngine(
        GuardrailConfig(
            rules=rules,
            stop_on_fatal=stop_on_fatal,
            enable_streaming=enable_streaming,
            check_interval=check_interval,
            on_violation=on_violation,
        )
    )


def check_guardrails(
    state: State, rules: list[GuardrailRule]
) -> list[GuardrailViolation]:
    """Run all guardrail rules against current state.

    Note: For the full GuardrailResult with shouldRetry/shouldHalt,
    use GuardrailEngine.check() or check_guardrails_full() instead.
    """
    violations = []
    for rule in rules:
        result = rule.check(state)
        if result:
            logger.debug(f"Guardrail '{rule.name}' triggered: {len(result)} violations")
        violations.extend(result)
    return violations


def check_guardrails_full(
    context: GuardrailContext | State,
    rules: list[GuardrailRule],
) -> GuardrailResult:
    """Execute rules once and return full result.

    Args:
        context: GuardrailContext or State object.
        rules: List of guardrail rules.

    Returns:
        GuardrailResult with violations and flags.

    Example:
        >>> result = check_guardrails_full(
        ...     GuardrailContext(content='{}', completed=True),
        ...     [json_rule()]
        ... )
        >>> result.passed
        True
    """
    engine = create_guardrail_engine(rules)
    return engine.check(context)


# ─────────────────────────────────────────────────────────────────────────────
# Async Guardrail Check Functions
# ─────────────────────────────────────────────────────────────────────────────


def run_async_guardrail_check(
    engine: GuardrailEngine,
    context: GuardrailContext,
    on_complete: Callable[[GuardrailResult], None],
) -> GuardrailResult | None:
    """Run guardrail check with fast/slow path.

    This implements the same fast/slow path pattern as TypeScript:
    - Try fast check first (delta-only, cheap)
    - If inconclusive, schedule full check async and call on_complete when done
    - Never blocks the main thread for large content

    Note: In Python, this uses call_soon() for consistency with the TypeScript
    setImmediate() pattern. For true async, use run_guardrail_check_async().

    Args:
        engine: The guardrail engine.
        context: The guardrail context.
        on_complete: Callback when async check completes.

    Returns:
        Immediate result if fast path succeeds, None if deferred to async.

    Example:
        >>> def handle_result(result):
        ...     if result.should_halt:
        ...         print("Halting!")
        >>> engine = create_guardrail_engine([json_rule()])
        >>> result = run_async_guardrail_check(engine, context, handle_result)
        >>> if result is not None:
        ...     print("Fast path:", result.passed)
    """
    import asyncio

    # Fast path: check delta only for obvious violations
    # This catches things like blocked words, obvious pattern matches
    if context.delta and len(context.delta) < 1000:
        quick_context = GuardrailContext(
            content=context.delta,  # Only check the delta
            completed=context.completed,
            checkpoint=context.checkpoint,
            delta=context.delta,
            token_count=context.token_count,
            metadata=context.metadata,
            previous_violations=context.previous_violations,
        )

        quick_result = engine.check(quick_context)

        # If we found violations in delta, return immediately
        if quick_result.violations:
            return quick_result

        # If delta is clean and content is small, do full check sync
        if len(context.content) < 5000:
            return engine.check(context)

    # Slow path: defer full content check
    # Use call_soon if we have an event loop, otherwise run sync
    try:
        loop = asyncio.get_running_loop()

        def _run_check() -> None:
            try:
                result = engine.check(context)
                on_complete(result)
            except Exception as e:
                # On error, fail safe - treat as a violation to prevent invalid content
                logger.error(f"Guardrail check failed with error: {e}")
                on_complete(
                    GuardrailResult(
                        passed=False,
                        violations=[
                            GuardrailViolation(
                                rule="internal_error",
                                severity="error",
                                message=f"Guardrail check failed: {e}",
                                recoverable=True,
                                timestamp=time.time(),
                            )
                        ],
                        should_retry=True,
                        should_halt=False,
                        summary=GuardrailResultSummary(
                            total=1, fatal=0, errors=1, warnings=0
                        ),
                    )
                )

        loop.call_soon(_run_check)
        return None  # Deferred to async
    except RuntimeError:
        # No event loop running - run synchronously
        result = engine.check(context)
        on_complete(result)
        return result


async def run_guardrail_check_async(
    engine: GuardrailEngine,
    context: GuardrailContext,
) -> GuardrailResult:
    """Run guardrail check asynchronously.

    This is the async/await version that always runs asynchronously.
    Use this in async contexts for cleaner code.

    Args:
        engine: The guardrail engine.
        context: The guardrail context.

    Returns:
        GuardrailResult from the check.

    Example:
        >>> async def check():
        ...     engine = create_guardrail_engine([json_rule()])
        ...     context = GuardrailContext(content='{}', completed=True)
        ...     result = await run_guardrail_check_async(engine, context)
        ...     return result.passed
    """
    import asyncio

    # Yield control to event loop, then run check
    await asyncio.sleep(0)
    try:
        return engine.check(context)
    except Exception as e:
        # On error, fail safe - treat as a violation to prevent invalid content
        logger.error(f"Guardrail check failed with error: {e}")
        return GuardrailResult(
            passed=False,
            violations=[
                GuardrailViolation(
                    rule="internal_error",
                    severity="error",
                    message=f"Guardrail check failed: {e}",
                    recoverable=True,
                    timestamp=time.time(),
                )
            ],
            should_retry=True,
            should_halt=False,
            summary=GuardrailResultSummary(total=1, fatal=0, errors=1, warnings=0),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Bad Patterns - Categorized
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class BadPatterns:
    """Categories of bad patterns to detect in LLM output."""

    # Meta commentary about being an AI
    META_COMMENTARY: list[str] = field(
        default_factory=lambda: [
            r"\bas an ai\b",
            r"\bas an artificial intelligence\b",
            r"\bi'?m an ai\b",
            r"\bi am an ai\b",
            r"\bas a language model\b",
            r"\bas an llm\b",
            r"\bi'?m a language model\b",
            r"\bi am a language model\b",
            r"\bas an ai assistant\b",
            r"\bi'?m an ai assistant\b",
        ]
    )

    # Hedging and filler phrases
    HEDGING: list[str] = field(
        default_factory=lambda: [
            r"^sure[,!]?\s",
            r"^certainly[,!]?\s",
            r"^of course[,!]?\s",
            r"^absolutely[,!]?\s",
            r"^definitely[,!]?\s",
            r"^great question[,!]?\s",
            r"^good question[,!]?\s",
            r"^that'?s a great question\b",
            r"^that'?s a good question\b",
            r"^i'?d be happy to\b",
            r"^i would be happy to\b",
        ]
    )

    # Refusal patterns
    REFUSAL: list[str] = field(
        default_factory=lambda: [
            r"\bi cannot provide\b",
            r"\bi can'?t provide\b",
            r"\bi'?m not able to\b",
            r"\bi am not able to\b",
            r"\bi cannot assist\b",
            r"\bi can'?t assist\b",
            r"\bi'?m unable to\b",
            r"\bi am unable to\b",
            r"\bi cannot help with\b",
            r"\bi can'?t help with\b",
            r"\bi must decline\b",
            r"\bi have to decline\b",
        ]
    )

    # Instruction/prompt leakage
    INSTRUCTION_LEAK: list[str] = field(
        default_factory=lambda: [
            r"\[SYSTEM\]",
            r"\[INST\]",
            r"\[/INST\]",
            r"<\|im_start\|>",
            r"<\|im_end\|>",
            r"<\|system\|>",
            r"<\|user\|>",
            r"<\|assistant\|>",
            r"<<SYS>>",
            r"<</SYS>>",
            r"\[AVAILABLE_TOOLS\]",
            r"\[/AVAILABLE_TOOLS\]",
        ]
    )

    # Placeholder patterns
    PLACEHOLDERS: list[str] = field(
        default_factory=lambda: [
            r"\[INSERT[^\]]*\]",
            r"\[YOUR[^\]]*\]",
            r"\[PLACEHOLDER[^\]]*\]",
            r"\[TODO[^\]]*\]",
            r"\[FILL[^\]]*\]",
            r"\{\{[^}]+\}\}",
            r"<PLACEHOLDER>",
            r"<INSERT>",
            r"<YOUR[^>]*>",
        ]
    )

    # Format collapse patterns
    FORMAT_COLLAPSE: list[str] = field(
        default_factory=lambda: [
            r"^here is the\b",
            r"^here'?s the\b",
            r"^let me\b",
            r"^i will now\b",
            r"^i'?ll now\b",
            r"^below is\b",
            r"^the following is\b",
            r"^please find\b",
        ]
    )

    def all_patterns(self) -> list[str]:
        """Get all patterns from all categories."""
        return (
            self.META_COMMENTARY
            + self.HEDGING
            + self.REFUSAL
            + self.INSTRUCTION_LEAK
            + self.PLACEHOLDERS
            + self.FORMAT_COLLAPSE
        )


# Singleton instance
BAD_PATTERNS = BadPatterns()


# ─────────────────────────────────────────────────────────────────────────────
# Analysis Functions
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class IncrementalJsonState:
    """State for incremental JSON parsing (O(delta) per token instead of O(content))."""

    open_braces: int = 0
    close_braces: int = 0
    open_brackets: int = 0
    close_brackets: int = 0
    in_string: bool = False
    escape_next: bool = False
    processed_length: int = 0


def update_json_state_incremental(
    state: IncrementalJsonState,
    delta: str,
) -> IncrementalJsonState:
    """Update JSON state incrementally with new delta content.

    Only processes the delta, not the full content - O(delta) per call.

    Args:
        state: Current incremental state
        delta: New content to process

    Returns:
        Updated state (mutates and returns the same object)
    """
    for char in delta:
        if state.escape_next:
            state.escape_next = False
            continue

        if char == "\\" and state.in_string:
            state.escape_next = True
            continue

        if char == '"' and not state.escape_next:
            state.in_string = not state.in_string
            continue

        if state.in_string:
            continue

        if char == "{":
            state.open_braces += 1
        elif char == "}":
            state.close_braces += 1
        elif char == "[":
            state.open_brackets += 1
        elif char == "]":
            state.close_brackets += 1

    state.processed_length += len(delta)
    return state


@dataclass
class JsonAnalysis:
    """Result of JSON structure analysis."""

    is_balanced: bool
    open_braces: int
    close_braces: int
    open_brackets: int
    close_brackets: int
    in_string: bool
    unclosed_string: bool
    issues: list[str] = field(default_factory=list)


def analyze_json_structure(content: str) -> JsonAnalysis:
    """Analyze JSON structure for balance and issues.

    Args:
        content: The content to analyze.

    Returns:
        JsonAnalysis with detailed structure information.

    Example:
        >>> result = analyze_json_structure('{"a": 1')
        >>> result.is_balanced
        False
        >>> result.open_braces
        1
        >>> result.close_braces
        0
    """
    open_braces = 0
    close_braces = 0
    open_brackets = 0
    close_brackets = 0
    in_string = False
    escape_next = False
    issues: list[str] = []
    last_char = ""
    consecutive_commas = 0

    for i, char in enumerate(content):
        if escape_next:
            escape_next = False
            last_char = char
            continue

        if char == "\\" and in_string:
            escape_next = True
            last_char = char
            continue

        if char == '"' and not escape_next:
            in_string = not in_string
            last_char = char
            continue

        if in_string:
            last_char = char
            continue

        # Track braces and brackets
        if char == "{":
            open_braces += 1
            if last_char == ",":
                issues.append(f"Malformed pattern ',{{' at position {i}")
        elif char == "}":
            close_braces += 1
        elif char == "[":
            open_brackets += 1
            if last_char == ",":
                issues.append(f"Malformed pattern ',[' at position {i}")
        elif char == "]":
            close_brackets += 1
        elif char == ",":
            consecutive_commas += 1
            if consecutive_commas > 1:
                issues.append(f"Multiple consecutive commas at position {i}")
        else:
            if not char.isspace():
                consecutive_commas = 0

        last_char = char

    # Check for unclosed string
    unclosed_string = in_string

    if unclosed_string:
        issues.append("Unclosed string detected")

    # Check balance
    is_balanced = (
        open_braces == close_braces
        and open_brackets == close_brackets
        and not unclosed_string
    )

    if open_braces > close_braces:
        issues.append(f"Unbalanced braces: {open_braces} open, {close_braces} close")
    elif open_braces < close_braces:
        issues.append(
            f"Too many closing braces: {close_braces} close vs {open_braces} open"
        )

    if open_brackets > close_brackets:
        issues.append(
            f"Unbalanced brackets: {open_brackets} open, {close_brackets} close"
        )
    elif open_brackets < close_brackets:
        issues.append(
            f"Too many closing brackets: {close_brackets} close vs {open_brackets} open"
        )

    return JsonAnalysis(
        is_balanced=is_balanced,
        open_braces=open_braces,
        close_braces=close_braces,
        open_brackets=open_brackets,
        close_brackets=close_brackets,
        in_string=in_string,
        unclosed_string=unclosed_string,
        issues=issues,
    )


def looks_like_json(content: str) -> bool:
    """Check if content looks like it's trying to be JSON.

    Args:
        content: The content to check.

    Returns:
        True if content appears to be JSON.
    """
    stripped = content.strip()
    if not stripped:
        return False
    return stripped.startswith(("{", "[")) or stripped.endswith(("}", "]"))


@dataclass
class MarkdownAnalysis:
    """Result of Markdown structure analysis."""

    is_balanced: bool
    in_fence: bool
    open_fences: int
    close_fences: int
    fence_languages: list[str]
    table_rows: int
    inconsistent_columns: bool
    issues: list[str] = field(default_factory=list)


def analyze_markdown_structure(content: str) -> MarkdownAnalysis:
    """Analyze Markdown structure for issues.

    Args:
        content: The content to analyze.

    Returns:
        MarkdownAnalysis with detailed structure information.

    Example:
        >>> result = analyze_markdown_structure("```js\\ncode")
        >>> result.in_fence
        True
        >>> result.open_fences
        1
    """
    lines = content.split("\n")
    open_fences = 0
    close_fences = 0
    in_fence = False
    fence_languages: list[str] = []
    issues: list[str] = []

    # Table analysis
    table_rows = 0
    table_columns: list[int] = []
    in_table = False
    inconsistent_columns = False

    fence_pattern = re.compile(r"^(`{3,}|~{3,})(\w*)")

    for i, line in enumerate(lines):
        # Check for code fences
        fence_match = fence_pattern.match(line.strip())
        if fence_match:
            fence_marker = fence_match.group(1)
            lang = fence_match.group(2)

            if not in_fence:
                in_fence = True
                open_fences += 1
                if lang:
                    fence_languages.append(lang)
            else:
                in_fence = False
                close_fences += 1

        # Skip content inside fences for other checks
        if in_fence:
            continue

        # Check for tables
        if "|" in line and line.strip().startswith("|"):
            cols = line.count("|") - 1  # Subtract 1 for trailing pipe
            if not in_table:
                in_table = True
                table_columns.append(cols)
            else:
                table_rows += 1
                if table_columns and cols != table_columns[-1]:
                    # Allow separator rows with different structure
                    if not re.match(r"^\|[\s:-]+\|", line.strip()):
                        inconsistent_columns = True
                        issues.append(f"Inconsistent table columns at line {i + 1}")
        else:
            if in_table:
                in_table = False
                table_columns = []

    # Check for unclosed fences
    if in_fence:
        issues.append("Unclosed code fence")

    is_balanced = open_fences == close_fences

    return MarkdownAnalysis(
        is_balanced=is_balanced,
        in_fence=in_fence,
        open_fences=open_fences,
        close_fences=close_fences,
        fence_languages=fence_languages,
        table_rows=table_rows,
        inconsistent_columns=inconsistent_columns,
        issues=issues,
    )


def looks_like_markdown(content: str) -> bool:
    """Check if content looks like Markdown.

    Args:
        content: The content to check.

    Returns:
        True if content appears to be Markdown.
    """
    patterns = [
        r"^#{1,6}\s",  # Headers
        r"```",  # Code fences
        r"^\s*[-*+]\s",  # Lists
        r"^\s*\d+\.\s",  # Numbered lists
        r"\[.*\]\(.*\)",  # Links
        r"^\|.*\|",  # Tables
        r"\*\*.*\*\*",  # Bold
        r"__.*__",  # Bold
        r"\*[^*]+\*",  # Italic
        r"_[^_]+_",  # Italic
    ]
    for pattern in patterns:
        if re.search(pattern, content, re.MULTILINE):
            return True
    return False


@dataclass
class LatexAnalysis:
    """Result of LaTeX structure analysis."""

    is_balanced: bool
    open_environments: list[str]
    display_math_balanced: bool
    inline_math_balanced: bool
    bracket_math_balanced: bool
    issues: list[str] = field(default_factory=list)


def analyze_latex_structure(content: str) -> LatexAnalysis:
    """Analyze LaTeX structure for balance and issues.

    Args:
        content: The content to analyze.

    Returns:
        LatexAnalysis with detailed structure information.

    Example:
        >>> result = analyze_latex_structure("\\\\begin{equation}")
        >>> result.open_environments
        ['equation']
        >>> result.is_balanced
        False
    """
    issues: list[str] = []
    open_environments: list[str] = []

    # Find all \begin{env} and \end{env} (including starred/hyphenated like align*)
    begin_pattern = re.compile(r"\\begin\{([^}]+)\}")
    end_pattern = re.compile(r"\\end\{([^}]+)\}")

    begins = [(m.group(1), m.start()) for m in begin_pattern.finditer(content)]
    ends = [(m.group(1), m.start()) for m in end_pattern.finditer(content)]

    # Track environment stack
    env_stack: list[str] = []
    all_events = sorted(
        [(pos, "begin", env) for env, pos in begins]
        + [(pos, "end", env) for env, pos in ends],
        key=lambda x: x[0],
    )

    for pos, event_type, env in all_events:
        if event_type == "begin":
            env_stack.append(env)
        else:  # end
            if not env_stack:
                issues.append(f"Unexpected \\end{{{env}}} without matching \\begin")
            elif env_stack[-1] != env:
                issues.append(
                    f"Mismatched environment: expected \\end{{{env_stack[-1]}}}, got \\end{{{env}}}"
                )
                env_stack.pop()
            else:
                env_stack.pop()

    open_environments = env_stack.copy()
    if open_environments:
        issues.append(f"Unclosed environments: {', '.join(open_environments)}")

    # Check display math $$...$$
    dollar_count = len(re.findall(r"(?<!\$)\$\$(?!\$)", content))
    display_math_balanced = dollar_count % 2 == 0
    if not display_math_balanced:
        issues.append("Unbalanced display math ($$)")

    # Check bracket math \[...\]
    open_bracket = len(re.findall(r"\\\[", content))
    close_bracket = len(re.findall(r"\\\]", content))
    bracket_math_balanced = open_bracket == close_bracket
    if not bracket_math_balanced:
        issues.append(
            f"Unbalanced bracket math: {open_bracket} \\[ vs {close_bracket} \\]"
        )

    # Check inline math $...$ (excluding $$)
    # This is tricky because $ is used for both open and close
    # Count single $ not preceded or followed by another $
    singles = re.findall(r"(?<![\\$])\$(?!\$)", content)
    inline_math_balanced = len(singles) % 2 == 0
    if not inline_math_balanced:
        issues.append("Unbalanced inline math ($)")

    is_balanced = (
        len(open_environments) == 0
        and display_math_balanced
        and inline_math_balanced
        and bracket_math_balanced
    )

    return LatexAnalysis(
        is_balanced=is_balanced,
        open_environments=open_environments,
        display_math_balanced=display_math_balanced,
        inline_math_balanced=inline_math_balanced,
        bracket_math_balanced=bracket_math_balanced,
        issues=issues,
    )


def looks_like_latex(content: str) -> bool:
    """Check if content looks like LaTeX.

    Args:
        content: The content to check.

    Returns:
        True if content appears to be LaTeX.
    """
    patterns = [
        r"\\begin\{",
        r"\\end\{",
        r"\$\$",
        r"\\\[",
        r"\\\]",
        r"\\frac\{",
        r"\\sum",
        r"\\int",
        r"\\alpha",
        r"\\beta",
        r"\\gamma",
        r"\\documentclass",
        r"\\usepackage",
    ]
    for pattern in patterns:
        if re.search(pattern, content):
            return True
    return False


def is_zero_output(content: str) -> bool:
    """Check if content is effectively empty.

    Args:
        content: The content to check.

    Returns:
        True if content is empty, whitespace-only, or meaningless.
    """
    if not content:
        return True
    if not content.strip():
        return True
    return False


def is_noise_only(content: str) -> bool:
    """Check if content is just noise (punctuation, repeated chars).

    Args:
        content: The content to check.

    Returns:
        True if content appears to be noise.
    """
    stripped = content.strip()
    if not stripped:
        return True

    # Check if only punctuation and whitespace
    if re.match(r"^[\s\.,!?;:\-_=+*#@&%$^(){}[\]<>/\\|`~\"\']+$", stripped):
        return True

    # Check for repeated single character
    if len(set(stripped.replace(" ", ""))) <= 2 and len(stripped) > 10:
        return True

    # Check for repeated short pattern
    if len(stripped) > 20:
        chunk = stripped[:10]
        if stripped.count(chunk) > len(stripped) / 15:
            return True

    return False


def find_bad_patterns(
    content: str,
    patterns: list[str],
) -> list[tuple[str, re.Match[str]]]:
    """Find all matches of bad patterns in content.

    Args:
        content: The content to search.
        patterns: List of regex patterns to search for.

    Returns:
        List of (pattern, match) tuples for all matches found.
    """
    matches = []
    for pattern in patterns:
        for match in re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE):
            matches.append((pattern, match))
    return matches


# ─────────────────────────────────────────────────────────────────────────────
# Pattern Detection Functions
# ─────────────────────────────────────────────────────────────────────────────


def detect_meta_commentary(
    context: GuardrailContext | State,
) -> list[GuardrailViolation]:
    """Detect meta commentary in output.

    Args:
        context: Guardrail context or State.

    Returns:
        List of violations for meta commentary patterns.

    Example:
        >>> ctx = GuardrailContext(content="As an AI, I cannot...", completed=True)
        >>> violations = detect_meta_commentary(ctx)
        >>> len(violations) > 0
        True
    """
    content = context.content
    violations: list[GuardrailViolation] = []

    matches = find_bad_patterns(content, BAD_PATTERNS.META_COMMENTARY)

    for pattern, match in matches:
        violations.append(
            GuardrailViolation(
                rule="pattern-meta-commentary",
                message=f'Meta commentary detected: "{match.group()}"',
                severity="error",
                position=match.start(),
                recoverable=True,
                suggestion="Retry generation without meta commentary",
            )
        )

    return violations


def detect_excessive_hedging(
    context: GuardrailContext | State,
) -> list[GuardrailViolation]:
    """Detect excessive hedging at start of content.

    Args:
        context: Guardrail context or State.

    Returns:
        List of violations for hedging patterns.

    Example:
        >>> ctx = GuardrailContext(content="Sure! Here is the answer...", completed=True)
        >>> violations = detect_excessive_hedging(ctx)
    """
    content = context.content
    violations: list[GuardrailViolation] = []

    # Check if content starts with hedging
    first_line = content.strip().split("\n")[0] if content.strip() else ""
    matches = find_bad_patterns(first_line, BAD_PATTERNS.HEDGING)

    if matches:
        pattern, match = matches[0]
        violations.append(
            GuardrailViolation(
                rule="pattern-hedging",
                message=f'Excessive hedging at start: "{match.group()}"',
                severity="warning",
                position=match.start(),
                recoverable=True,
                suggestion="Content should start directly without hedging",
            )
        )

    return violations


def detect_refusal(
    context: GuardrailContext | State,
) -> list[GuardrailViolation]:
    """Detect refusal patterns in content.

    Args:
        context: Guardrail context or State.

    Returns:
        List of violations for refusal patterns.

    Example:
        >>> ctx = GuardrailContext(content="I cannot provide that information", completed=True)
        >>> violations = detect_refusal(ctx)
        >>> len(violations) > 0
        True
    """
    content = context.content
    violations: list[GuardrailViolation] = []

    matches = find_bad_patterns(content, BAD_PATTERNS.REFUSAL)

    for pattern, match in matches:
        violations.append(
            GuardrailViolation(
                rule="pattern-refusal",
                message=f'Refusal pattern detected: "{match.group()}"',
                severity="error",
                position=match.start(),
                recoverable=False,
                suggestion="Model refused to complete the task",
            )
        )

    return violations


def detect_instruction_leakage(
    context: GuardrailContext | State,
) -> list[GuardrailViolation]:
    """Detect instruction/prompt leakage in content.

    Args:
        context: Guardrail context or State.

    Returns:
        List of violations for instruction leak patterns.

    Example:
        >>> ctx = GuardrailContext(content="[SYSTEM] You are...", completed=True)
        >>> violations = detect_instruction_leakage(ctx)
        >>> len(violations) > 0
        True
    """
    content = context.content
    violations: list[GuardrailViolation] = []

    matches = find_bad_patterns(content, BAD_PATTERNS.INSTRUCTION_LEAK)

    for pattern, match in matches:
        violations.append(
            GuardrailViolation(
                rule="pattern-instruction-leak",
                message=f'Instruction leakage detected: "{match.group()}"',
                severity="error",
                position=match.start(),
                recoverable=True,
                suggestion="Retry generation without system tokens",
            )
        )

    return violations


def detect_placeholders(
    context: GuardrailContext | State,
) -> list[GuardrailViolation]:
    """Detect placeholder patterns in content.

    Only checks complete output.

    Args:
        context: Guardrail context or State.

    Returns:
        List of violations for placeholder patterns.

    Example:
        >>> ctx = GuardrailContext(content="Hello [INSERT NAME HERE]", completed=True)
        >>> violations = detect_placeholders(ctx)
        >>> len(violations) > 0
        True
    """
    content = context.content
    completed = context.completed
    violations: list[GuardrailViolation] = []

    # Only check complete output
    if not completed:
        return violations

    matches = find_bad_patterns(content, BAD_PATTERNS.PLACEHOLDERS)

    for pattern, match in matches:
        violations.append(
            GuardrailViolation(
                rule="pattern-placeholders",
                message=f'Placeholder detected: "{match.group()}"',
                severity="error",
                position=match.start(),
                recoverable=True,
                suggestion="Output contains incomplete placeholders",
            )
        )

    return violations


def detect_format_collapse(
    context: GuardrailContext | State,
) -> list[GuardrailViolation]:
    """Detect format collapse (mixing instructions with output).

    Args:
        context: Guardrail context or State.

    Returns:
        List of violations for format collapse patterns.

    Example:
        >>> ctx = GuardrailContext(content="Here is the code:", completed=True)
        >>> violations = detect_format_collapse(ctx)
    """
    content = context.content
    violations: list[GuardrailViolation] = []

    # Only check beginning of content
    first_lines = "\n".join(content.split("\n")[:3])
    matches = find_bad_patterns(first_lines, BAD_PATTERNS.FORMAT_COLLAPSE)

    if matches:
        pattern, match = matches[0]
        violations.append(
            GuardrailViolation(
                rule="pattern-format-collapse",
                message=f'Format collapse detected: "{match.group()}"',
                severity="warning",
                position=match.start(),
                recoverable=True,
                suggestion="Output should not mix meta-instructions with content",
            )
        )

    return violations


def detect_repetition(
    context: GuardrailContext | State,
    threshold: int = 2,
) -> list[GuardrailViolation]:
    """Detect repeated sentences or paragraphs.

    Only checks complete output.

    Args:
        context: Guardrail context or State.
        threshold: Number of repetitions to trigger (default 2).

    Returns:
        List of violations for repetition patterns.

    Example:
        >>> ctx = GuardrailContext(
        ...     content="This is a test. This is a test. This is a test.",
        ...     completed=True
        ... )
        >>> violations = detect_repetition(ctx)
        >>> len(violations) > 0
        True
    """
    content = context.content
    completed = context.completed
    violations: list[GuardrailViolation] = []

    # Only check complete output
    if not completed:
        return violations

    # Split into sentences
    sentences = [
        s.strip().lower()
        for s in re.split(r"[.!?]+", content)
        if s.strip() and len(s.strip()) > 20  # Only check substantial sentences
    ]

    # Count occurrences
    from collections import Counter

    counts = Counter(sentences)

    # Find repeated sentences
    for sentence, count in counts.items():
        if count > threshold:
            violations.append(
                GuardrailViolation(
                    rule="pattern-repetition",
                    message=f'Sentence repeated {count} times: "{sentence[:50]}..."',
                    severity="error",
                    recoverable=True,
                    suggestion="Content contains repeated sentences",
                    context={"sentence": sentence, "count": count},
                )
            )

    return violations


def detect_first_last_duplicate(
    context: GuardrailContext | State,
) -> list[GuardrailViolation]:
    """Detect duplicated first and last sentence.

    This is a common sign of model looping. Only checks complete output
    with sufficient content.

    Args:
        context: Guardrail context or State.

    Returns:
        List of violations if first and last sentences match.

    Example:
        >>> ctx = GuardrailContext(
        ...     content="Hello world. Some middle content. Hello world.",
        ...     completed=True
        ... )
        >>> violations = detect_first_last_duplicate(ctx)
        >>> len(violations) > 0
        True
    """
    content = context.content
    completed = context.completed
    violations: list[GuardrailViolation] = []

    # Only check complete output with sufficient content
    if not completed or len(content) < 100:
        return violations

    sentences = [
        s.strip()
        for s in re.split(r"[.!?]+", content)
        if s.strip() and len(s.strip()) > 10
    ]

    if len(sentences) < 2:
        return violations

    first = sentences[0].lower()
    last = sentences[-1].lower()

    if first == last:
        violations.append(
            GuardrailViolation(
                rule="pattern-first-last-duplicate",
                message="First and last sentences are identical",
                severity="error",
                recoverable=True,
                suggestion="Retry generation - possible loop detected",
            )
        )

    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Built-in Rules
# ─────────────────────────────────────────────────────────────────────────────


def json_rule() -> GuardrailRule:
    """Check for balanced JSON structure during streaming.

    Detects:
    - Unbalanced {} and []
    - Unclosed strings
    - Multiple consecutive commas
    - Malformed patterns like {, or [,

    Uses incremental state tracking for O(delta) per-token updates instead of
    O(content) full scans during streaming. Only does full analysis at completion.

    Note: State is reset when content is empty or shorter than processed length
    to handle new streams, aborted streams, or rule reuse.
    """
    # Incremental state for O(delta) streaming checks
    incremental_state = IncrementalJsonState()
    last_content_length = 0
    is_json_content: bool | None = None  # Cache: None=unknown, True/False=determined

    def check(state: State) -> list[GuardrailViolation]:
        nonlocal incremental_state, last_content_length, is_json_content

        content = state.content
        if not content.strip():
            # Reset state when content is empty (new stream starting)
            incremental_state = IncrementalJsonState()
            last_content_length = 0
            is_json_content = None
            return []

        # Only check if it looks like JSON (cache after first determination)
        if is_json_content is None:
            is_json_content = looks_like_json(content)
        if not is_json_content:
            # Reset state when content doesn't look like JSON
            incremental_state = IncrementalJsonState()
            last_content_length = 0
            return []

        # Reset state if content is shorter than what we've processed
        # (indicates a new stream or aborted stream being reused)
        if len(content) < last_content_length:
            incremental_state = IncrementalJsonState()
            last_content_length = 0
            is_json_content = None

        violations = []

        # During streaming, use incremental state tracking (O(delta) instead of O(content))
        if not state.completed:
            # Get delta since last check
            if len(content) > last_content_length:
                delta = content[last_content_length:]
                update_json_state_incremental(incremental_state, delta)
                last_content_length = len(content)

            # Check for critical issues using incremental state
            if incremental_state.close_braces > incremental_state.open_braces:
                violations.append(
                    GuardrailViolation(
                        rule="json",
                        message="Too many closing braces",
                        severity="error",
                        suggestion="Check JSON structure",
                    )
                )
            if incremental_state.close_brackets > incremental_state.open_brackets:
                violations.append(
                    GuardrailViolation(
                        rule="json",
                        message="Too many closing brackets",
                        severity="error",
                        suggestion="Check JSON structure",
                    )
                )
        else:
            # On completion, do full analysis for comprehensive check
            analysis = analyze_json_structure(content)

            # Check for both extra closes AND missing closes
            if analysis.close_braces > analysis.open_braces:
                violations.append(
                    GuardrailViolation(
                        rule="json",
                        message=f"Too many closing braces: {analysis.close_braces} close vs {analysis.open_braces} open",
                        severity="error",
                    )
                )
            elif analysis.open_braces > analysis.close_braces:
                violations.append(
                    GuardrailViolation(
                        rule="json",
                        message=f"Missing closing braces: {analysis.open_braces} open vs {analysis.close_braces} close",
                        severity="error",
                    )
                )
            if analysis.close_brackets > analysis.open_brackets:
                violations.append(
                    GuardrailViolation(
                        rule="json",
                        message=f"Too many closing brackets: {analysis.close_brackets} close vs {analysis.open_brackets} open",
                        severity="error",
                    )
                )
            elif analysis.open_brackets > analysis.close_brackets:
                violations.append(
                    GuardrailViolation(
                        rule="json",
                        message=f"Missing closing brackets: {analysis.open_brackets} open vs {analysis.close_brackets} close",
                        severity="error",
                    )
                )
            # Report other issues (unclosed strings, etc.)
            for issue in analysis.issues:
                if "Unbalanced" not in issue and "closing" not in issue.lower():
                    violations.append(
                        GuardrailViolation(
                            rule="json",
                            message=issue,
                            severity="error",
                        )
                    )

            # Reset incremental state for potential reuse
            incremental_state = IncrementalJsonState()
            last_content_length = 0

        return violations

    return GuardrailRule(
        name="json",
        check=check,
        description="Validates JSON structure during streaming",
    )


def strict_json_rule() -> GuardrailRule:
    """Validate complete JSON on completion.

    Requires:
    - Valid parseable JSON
    - Root must be object or array
    """

    def check(state: State) -> list[GuardrailViolation]:
        if not state.completed:
            return []

        content = state.content.strip()
        if not content:
            return [
                GuardrailViolation(
                    rule="strict_json",
                    message="Empty output, expected JSON",
                    severity="error",
                )
            ]

        # Check if it looks like JSON
        if not looks_like_json(content):
            return [
                GuardrailViolation(
                    rule="strict_json",
                    message="Output does not appear to be JSON",
                    severity="error",
                )
            ]

        try:
            parsed = json.loads(content)
            # Check root is object or array
            if not isinstance(parsed, (dict, list)):
                return [
                    GuardrailViolation(
                        rule="strict_json",
                        message=f"JSON root must be object or array, got {type(parsed).__name__}",
                        severity="error",
                    )
                ]
            return []
        except json.JSONDecodeError as e:
            return [
                GuardrailViolation(
                    rule="strict_json",
                    message=f"Invalid JSON: {e}",
                    severity="error",
                    position=e.pos,
                )
            ]

    return GuardrailRule(
        name="strict_json",
        check=check,
        streaming=False,
        description="Validates complete JSON is parseable",
    )


def markdown_rule() -> GuardrailRule:
    """Validate Markdown structure.

    Detects:
    - Unclosed code fences (```)
    - Inconsistent table columns
    """

    def check(state: State) -> list[GuardrailViolation]:
        # During streaming, markdown is always incomplete — skip expensive analysis
        if not state.completed:
            return []

        content = state.content
        if not content.strip():
            return []

        analysis = analyze_markdown_structure(content)
        violations = []

        if True:
            # On completion, report issues
            for issue in analysis.issues:
                severity: Severity = "warning"
                if "Unclosed" in issue:
                    severity = "error"
                violations.append(
                    GuardrailViolation(
                        rule="markdown",
                        message=issue,
                        severity=severity,
                    )
                )

        return violations

    return GuardrailRule(
        name="markdown",
        check=check,
        description="Validates Markdown structure",
    )


def latex_rule() -> GuardrailRule:
    """Validate LaTeX environments and math.

    Detects:
    - Unclosed \\begin{env} environments
    - Mismatched environment names
    - Unbalanced \\[...\\] and $$...$$
    - Unbalanced inline math $...$
    """

    def check(state: State) -> list[GuardrailViolation]:
        content = state.content
        if not content.strip():
            return []

        # Only check if it looks like LaTeX
        if not looks_like_latex(content):
            return []

        analysis = analyze_latex_structure(content)
        violations = []

        # During streaming, only report mismatches (not unclosed)
        if not state.completed:
            for issue in analysis.issues:
                if "Mismatched" in issue or "Unexpected" in issue:
                    violations.append(
                        GuardrailViolation(
                            rule="latex",
                            message=issue,
                            severity="error",
                        )
                    )
        else:
            # On completion, report all issues
            for issue in analysis.issues:
                violations.append(
                    GuardrailViolation(
                        rule="latex",
                        message=issue,
                        severity="error",
                    )
                )

        return violations

    return GuardrailRule(
        name="latex",
        check=check,
        description="Validates LaTeX environments and math",
    )


def pattern_rule(
    patterns: list[str] | None = None,
    *,
    include_categories: list[str] | None = None,
    exclude_categories: list[str] | None = None,
) -> GuardrailRule:
    """Detect unwanted patterns in output.

    Args:
        patterns: Custom patterns to use. If None, uses BAD_PATTERNS based on categories.
        include_categories: Categories to include (META_COMMENTARY, HEDGING, REFUSAL,
                          INSTRUCTION_LEAK, PLACEHOLDERS, FORMAT_COLLAPSE). If None, uses all.
        exclude_categories: Categories to exclude.

    Built-in pattern categories:
    - META_COMMENTARY: "As an AI...", "I'm an AI assistant"
    - HEDGING: "Sure!", "Certainly!", "Of course!"
    - REFUSAL: "I cannot provide...", "I'm not able to..."
    - INSTRUCTION_LEAK: [SYSTEM], <|im_start|>
    - PLACEHOLDERS: [INSERT ...], {{placeholder}}
    - FORMAT_COLLAPSE: "Here is the...", "Let me..."
    """
    if patterns is None:
        # Build patterns from categories
        all_categories = {
            "META_COMMENTARY": BAD_PATTERNS.META_COMMENTARY,
            "HEDGING": BAD_PATTERNS.HEDGING,
            "REFUSAL": BAD_PATTERNS.REFUSAL,
            "INSTRUCTION_LEAK": BAD_PATTERNS.INSTRUCTION_LEAK,
            "PLACEHOLDERS": BAD_PATTERNS.PLACEHOLDERS,
            "FORMAT_COLLAPSE": BAD_PATTERNS.FORMAT_COLLAPSE,
        }

        if include_categories:
            categories = {
                k: v for k, v in all_categories.items() if k in include_categories
            }
        else:
            categories = all_categories

        if exclude_categories:
            categories = {
                k: v for k, v in categories.items() if k not in exclude_categories
            }

        patterns = []
        for cat_patterns in categories.values():
            patterns.extend(cat_patterns)

    # Pre-compile all patterns into a single combined regex for O(1) pass
    combined = re.compile(
        "|".join(f"(?:{p})" for p in patterns) if patterns else r"(?!x)x",
        re.IGNORECASE | re.MULTILINE,
    )
    last_scanned_length = 0

    def check(state: State) -> list[GuardrailViolation]:
        nonlocal last_scanned_length
        content = state.content

        # Reset tracking if content was replaced (e.g. new stream)
        if len(content) < last_scanned_length:
            last_scanned_length = 0

        # On completion, do a full scan to catch anything delta scanning might miss
        # (e.g. ^-anchored patterns that span chunk boundaries).
        # During streaming, only scan new content + overlap for performance.
        if state.completed:
            scan_start = 0
        else:
            scan_start = max(0, last_scanned_length - 50)  # overlap for boundary matches
        scan_region = content[scan_start:]
        last_scanned_length = len(content)

        violations = []
        for match in combined.finditer(scan_region):
            violations.append(
                GuardrailViolation(
                    rule="pattern",
                    message=f"Matched unwanted pattern: {match.group()}",
                    severity="warning",
                    position=scan_start + match.start(),
                    context={"matched": match.group()},
                )
            )
        return violations

    return GuardrailRule(
        name="pattern",
        check=check,
        severity="warning",
        description="Detects unwanted patterns in output",
    )


def custom_pattern_rule(
    patterns: list[str],
    message: str = "Custom pattern violation",
    severity: Severity = "error",
) -> GuardrailRule:
    """Create a custom pattern rule.

    Args:
        patterns: List of regex patterns to detect.
        message: Message to show when pattern is matched.
        severity: Severity level for violations.

    Example:
        >>> rule = custom_pattern_rule([r"forbidden", r"blocked"], "Custom violation", "error")
    """

    def check(state: State) -> list[GuardrailViolation]:
        violations = []
        matches = find_bad_patterns(state.content, patterns)
        for pattern, match in matches:
            violations.append(
                GuardrailViolation(
                    rule="custom_pattern",
                    message=f"{message}: {match.group()}",
                    severity=severity,
                    position=match.start(),
                    context={"pattern": pattern, "matched": match.group()},
                )
            )
        return violations

    return GuardrailRule(
        name="custom_pattern",
        check=check,
        severity=severity,
        description=f"Custom pattern rule: {message}",
    )


def zero_output_rule() -> GuardrailRule:
    """Detect empty or meaningless output.

    Detects:
    - Empty output
    - Whitespace-only output
    - Punctuation-only output
    - Repeated character noise
    """

    def check(state: State) -> list[GuardrailViolation]:
        if not state.completed:
            return []

        violations = []

        # Check for zero/empty output
        if is_zero_output(state.content):
            violations.append(
                GuardrailViolation(
                    rule="zero_output",
                    message="Empty or whitespace-only output",
                    severity="error",
                )
            )
            return violations

        # Check for noise-only output
        if is_noise_only(state.content):
            violations.append(
                GuardrailViolation(
                    rule="zero_output",
                    message="Output appears to be noise (punctuation or repeated characters)",
                    severity="error",
                )
            )

        return violations

    return GuardrailRule(
        name="zero_output",
        check=check,
        streaming=False,
        description="Detects empty or meaningless output",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Drift Detection
# ─────────────────────────────────────────────────────────────────────────────


def stall_rule(max_gap: float = 5.0) -> GuardrailRule:
    """Detect token stalls (no tokens for too long).

    Args:
        max_gap: Maximum seconds between tokens before triggering.
    """

    def check(state: State) -> list[GuardrailViolation]:
        if state.last_token_at is None:
            return []
        gap = time.time() - state.last_token_at
        if gap > max_gap:
            state.drift_detected = True
            return [
                GuardrailViolation(
                    rule="stall",
                    message=f"Token stall: {gap:.1f}s since last token",
                    severity="warning",
                    context={"gap_seconds": gap},
                )
            ]
        return []

    return GuardrailRule(
        name="stall",
        check=check,
        severity="warning",
        description="Detects token stalls",
    )


def repetition_rule(
    window: int = 100,
    threshold: float = 0.5,
    *,
    sentence_check: bool = True,
    sentence_repeat_count: int = 3,
) -> GuardrailRule:
    """Detect repetitive output (model looping).

    Args:
        window: Character window size for similarity check.
        threshold: Similarity threshold (0-1) to trigger.
        sentence_check: Also check for repeated sentences.
        sentence_repeat_count: Number of sentence repeats to trigger.
    """

    def check(state: State) -> list[GuardrailViolation]:
        content = state.content
        violations = []

        # Character-based similarity check
        if len(content) >= window * 2:
            recent = content[-window:]
            previous = content[-window * 2 : -window]

            matches = sum(1 for a, b in zip(recent, previous, strict=False) if a == b)
            similarity = matches / window

            if similarity > threshold:
                state.drift_detected = True
                violations.append(
                    GuardrailViolation(
                        rule="repetition",
                        message=f"Repetitive output detected ({similarity:.0%} character similarity)",
                        severity="error",
                        context={"similarity": similarity, "window": window},
                    )
                )

        # Sentence repetition check
        if sentence_check and len(content) > 50:
            # Split into sentences
            sentences = re.split(r"[.!?]+\s+", content)
            sentences = [s.strip() for s in sentences if s.strip()]

            if len(sentences) >= sentence_repeat_count:
                # Count sentence occurrences
                from collections import Counter

                counts = Counter(sentences)
                for sentence, count in counts.items():
                    if count >= sentence_repeat_count and len(sentence) > 20:
                        state.drift_detected = True
                        violations.append(
                            GuardrailViolation(
                                rule="repetition",
                                message=f"Sentence repeated {count} times",
                                severity="error",
                                context={
                                    "sentence": sentence[:50] + "...",
                                    "count": count,
                                },
                            )
                        )
                        break  # Only report once

        return violations

    return GuardrailRule(
        name="repetition",
        check=check,
        description="Detects repetitive output",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Presets (legacy functions - use Guardrails class instead)
# ─────────────────────────────────────────────────────────────────────────────


def recommended_guardrails() -> list[GuardrailRule]:
    """Recommended set of guardrails."""
    return [json_rule(), markdown_rule(), pattern_rule(), zero_output_rule()]


def strict_guardrails() -> list[GuardrailRule]:
    """Strict guardrails including drift detection."""
    return [
        json_rule(),
        strict_json_rule(),
        markdown_rule(),
        latex_rule(),
        pattern_rule(),
        zero_output_rule(),
        stall_rule(),
        repetition_rule(),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Guardrails class - Clean API
# ─────────────────────────────────────────────────────────────────────────────


class Guardrails:
    """Guardrails namespace for presets, rules, and analysis.

    Usage:
        # Presets
        guardrails = l0.Guardrails.recommended()
        guardrails = l0.Guardrails.strict()

        # Individual rules
        rules = [l0.Guardrails.json(), l0.Guardrails.pattern()]

        # Analysis
        result = l0.Guardrails.analyze_json('{"key": "value"}')

        # Patterns
        patterns = l0.Guardrails.BAD_PATTERNS.META_COMMENTARY
    """

    # ─────────────────────────────────────────────────────────────────────────
    # Patterns
    # ─────────────────────────────────────────────────────────────────────────

    BAD_PATTERNS = BAD_PATTERNS

    # ─────────────────────────────────────────────────────────────────────────
    # Types (for type hints)
    # ─────────────────────────────────────────────────────────────────────────

    Rule = GuardrailRule
    Violation = GuardrailViolation
    Context = GuardrailContext
    Result = GuardrailResult
    ResultSummary = GuardrailResultSummary
    State = GuardrailState
    Config = GuardrailConfig
    Engine = GuardrailEngine
    JsonAnalysis = JsonAnalysis
    MarkdownAnalysis = MarkdownAnalysis
    LatexAnalysis = LatexAnalysis

    # ─────────────────────────────────────────────────────────────────────────
    # Presets
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def minimal() -> list[GuardrailRule]:
        """Minimal guardrails - JSON + zero output only.

        Includes:
        - json: Check balanced JSON brackets
        - zero_output: Detect empty output
        """
        return [json_rule(), zero_output_rule()]

    @staticmethod
    def recommended() -> list[GuardrailRule]:
        """Recommended guardrails for most use cases.

        Includes:
        - json: Check balanced JSON brackets
        - markdown: Check Markdown structure
        - pattern: Detect AI slop patterns
        - zero_output: Detect empty output
        """
        return [json_rule(), markdown_rule(), pattern_rule(), zero_output_rule()]

    @staticmethod
    def strict() -> list[GuardrailRule]:
        """Strict guardrails including drift detection.

        Includes everything in recommended(), plus:
        - strict_json: Validate complete JSON
        - latex: Validate LaTeX structure
        - stall: Detect token stalls
        - repetition: Detect model looping
        """
        return [
            json_rule(),
            strict_json_rule(),
            markdown_rule(),
            latex_rule(),
            pattern_rule(),
            zero_output_rule(),
            stall_rule(),
            repetition_rule(),
        ]

    @staticmethod
    def json_only() -> list[GuardrailRule]:
        """JSON validation + zero output."""
        return [json_rule(), strict_json_rule(), zero_output_rule()]

    @staticmethod
    def markdown_only() -> list[GuardrailRule]:
        """Markdown validation + zero output."""
        return [markdown_rule(), zero_output_rule()]

    @staticmethod
    def latex_only() -> list[GuardrailRule]:
        """LaTeX validation + zero output."""
        return [latex_rule(), zero_output_rule()]

    @staticmethod
    def none() -> list[GuardrailRule]:
        """No guardrails (explicit opt-out)."""
        return []

    # ─────────────────────────────────────────────────────────────────────────
    # Rules
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def json() -> GuardrailRule:
        """Check for balanced JSON structure during streaming."""
        return json_rule()

    @staticmethod
    def strict_json() -> GuardrailRule:
        """Validate complete JSON on completion."""
        return strict_json_rule()

    @staticmethod
    def markdown() -> GuardrailRule:
        """Validate Markdown structure."""
        return markdown_rule()

    @staticmethod
    def latex() -> GuardrailRule:
        """Validate LaTeX environments and math."""
        return latex_rule()

    @staticmethod
    def pattern(
        patterns: list[str] | None = None,
        *,
        include_categories: list[str] | None = None,
        exclude_categories: list[str] | None = None,
    ) -> GuardrailRule:
        """Detect unwanted patterns in output.

        Args:
            patterns: Custom patterns. If None, uses BAD_PATTERNS.
            include_categories: Categories to include.
            exclude_categories: Categories to exclude.
        """
        return pattern_rule(
            patterns,
            include_categories=include_categories,
            exclude_categories=exclude_categories,
        )

    @staticmethod
    def custom_pattern(
        patterns: list[str],
        message: str = "Custom pattern violation",
        severity: Severity = "error",
    ) -> GuardrailRule:
        """Create a custom pattern rule.

        Args:
            patterns: List of regex patterns to detect.
            message: Message to show when pattern is matched.
            severity: Severity level for violations.
        """
        return custom_pattern_rule(patterns, message, severity)

    @staticmethod
    def zero_output() -> GuardrailRule:
        """Detect empty or meaningless output."""
        return zero_output_rule()

    @staticmethod
    def stall(max_gap: float = 5.0) -> GuardrailRule:
        """Detect token stalls.

        Args:
            max_gap: Maximum seconds between tokens.
        """
        return stall_rule(max_gap)

    @staticmethod
    def repetition(
        window: int = 100,
        threshold: float = 0.5,
        *,
        sentence_check: bool = True,
        sentence_repeat_count: int = 3,
    ) -> GuardrailRule:
        """Detect repetitive output.

        Args:
            window: Character window size.
            threshold: Similarity threshold (0-1).
            sentence_check: Check for repeated sentences.
            sentence_repeat_count: Number of repeats to trigger.
        """
        return repetition_rule(
            window,
            threshold,
            sentence_check=sentence_check,
            sentence_repeat_count=sentence_repeat_count,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Analysis Functions
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def analyze_json(content: str) -> "JsonAnalysis":
        """Analyze JSON structure for balance and issues."""
        return analyze_json_structure(content)

    @staticmethod
    def analyze_markdown(content: str) -> "MarkdownAnalysis":
        """Analyze Markdown structure for issues."""
        return analyze_markdown_structure(content)

    @staticmethod
    def analyze_latex(content: str) -> "LatexAnalysis":
        """Analyze LaTeX structure for balance and issues."""
        return analyze_latex_structure(content)

    @staticmethod
    def looks_like_json(content: str) -> bool:
        """Check if content looks like JSON."""
        return looks_like_json(content)

    @staticmethod
    def looks_like_markdown(content: str) -> bool:
        """Check if content looks like Markdown."""
        return looks_like_markdown(content)

    @staticmethod
    def looks_like_latex(content: str) -> bool:
        """Check if content looks like LaTeX."""
        return looks_like_latex(content)

    @staticmethod
    def is_zero_output(content: str) -> bool:
        """Check if content is effectively empty."""
        return is_zero_output(content)

    @staticmethod
    def is_noise_only(content: str) -> bool:
        """Check if content is just noise."""
        return is_noise_only(content)

    @staticmethod
    def find_patterns(
        content: str, patterns: list[str]
    ) -> list[tuple[str, re.Match[str]]]:
        """Find all matches of patterns in content."""
        return find_bad_patterns(content, patterns)

    # ─────────────────────────────────────────────────────────────────────────
    # Pattern Detection Functions
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def detect_meta_commentary(
        context: GuardrailContext | State,
    ) -> list[GuardrailViolation]:
        """Detect meta commentary in output."""
        return detect_meta_commentary(context)

    @staticmethod
    def detect_excessive_hedging(
        context: GuardrailContext | State,
    ) -> list[GuardrailViolation]:
        """Detect excessive hedging at start of content."""
        return detect_excessive_hedging(context)

    @staticmethod
    def detect_refusal(
        context: GuardrailContext | State,
    ) -> list[GuardrailViolation]:
        """Detect refusal patterns in content."""
        return detect_refusal(context)

    @staticmethod
    def detect_instruction_leakage(
        context: GuardrailContext | State,
    ) -> list[GuardrailViolation]:
        """Detect instruction/prompt leakage in content."""
        return detect_instruction_leakage(context)

    @staticmethod
    def detect_placeholders(
        context: GuardrailContext | State,
    ) -> list[GuardrailViolation]:
        """Detect placeholder patterns in content."""
        return detect_placeholders(context)

    @staticmethod
    def detect_format_collapse(
        context: GuardrailContext | State,
    ) -> list[GuardrailViolation]:
        """Detect format collapse in content."""
        return detect_format_collapse(context)

    @staticmethod
    def detect_repetition(
        context: GuardrailContext | State,
        threshold: int = 2,
    ) -> list[GuardrailViolation]:
        """Detect repeated sentences or paragraphs."""
        return detect_repetition(context, threshold)

    @staticmethod
    def detect_first_last_duplicate(
        context: GuardrailContext | State,
    ) -> list[GuardrailViolation]:
        """Detect duplicated first and last sentence."""
        return detect_first_last_duplicate(context)

    # ─────────────────────────────────────────────────────────────────────────
    # Engine Functions
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def create_engine(
        rules: list[GuardrailRule],
        *,
        stop_on_fatal: bool = True,
        enable_streaming: bool = True,
        check_interval: int = 100,
        on_violation: Callable[[GuardrailViolation], None] | None = None,
    ) -> GuardrailEngine:
        """Create a guardrail engine with rules.

        Args:
            rules: List of guardrail rules.
            stop_on_fatal: Whether to stop on first fatal violation.
            enable_streaming: Whether to run streaming checks.
            check_interval: Interval for streaming checks.
            on_violation: Callback when violation is detected.

        Returns:
            Configured GuardrailEngine.
        """
        return create_guardrail_engine(
            rules,
            stop_on_fatal=stop_on_fatal,
            enable_streaming=enable_streaming,
            check_interval=check_interval,
            on_violation=on_violation,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Check Functions
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def check(state: State, rules: list[GuardrailRule]) -> list[GuardrailViolation]:
        """Run all guardrail rules against current state.

        Note: For the full GuardrailResult with shouldRetry/shouldHalt,
        use check_full() or create an engine instead.
        """
        return check_guardrails(state, rules)

    @staticmethod
    def check_full(
        context: GuardrailContext | State,
        rules: list[GuardrailRule],
    ) -> GuardrailResult:
        """Execute rules once and return full result.

        Returns GuardrailResult with passed, violations, shouldRetry, shouldHalt.
        """
        return check_guardrails_full(context, rules)

    # ─────────────────────────────────────────────────────────────────────────
    # Async Check Functions
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def run_async_check(
        engine: GuardrailEngine,
        context: GuardrailContext,
        on_complete: Callable[[GuardrailResult], None],
    ) -> GuardrailResult | None:
        """Run guardrail check with fast/slow path.

        This implements the same fast/slow path pattern as TypeScript:
        - Try fast check first (delta-only, cheap)
        - If inconclusive, schedule full check async and call on_complete when done
        - Never blocks the main thread for large content

        Returns:
            Immediate result if fast path succeeds, None if deferred to async.
        """
        return run_async_guardrail_check(engine, context, on_complete)

    @staticmethod
    async def run_check_async(
        engine: GuardrailEngine,
        context: GuardrailContext,
    ) -> GuardrailResult:
        """Run guardrail check asynchronously.

        This is the async/await version that always runs asynchronously.
        Use this in async contexts for cleaner code.
        """
        return await run_guardrail_check_async(engine, context)


# ─────────────────────────────────────────────────────────────────────────────
# Guardrail Presets (TypeScript parity)
# ─────────────────────────────────────────────────────────────────────────────

# These match TypeScript's minimalGuardrails, recommendedGuardrails, etc.
# Note: These are functions that return fresh rule lists, not constants,
# because GuardrailRule instances have mutable state.


def minimal_guardrails() -> list[GuardrailRule]:
    """Minimal guardrails - JSON + zero output only.

    Matches TypeScript's minimalGuardrails preset.
    """
    return Guardrails.minimal()


# Preset constants for TypeScript API parity
# Use these like: guardrails=MINIMAL_GUARDRAILS()
MINIMAL_GUARDRAILS = minimal_guardrails
"""Minimal guardrails preset: json, zero_output."""

RECOMMENDED_GUARDRAILS = Guardrails.recommended
"""Recommended guardrails preset: json, markdown, pattern, zero_output."""

STRICT_GUARDRAILS = Guardrails.strict
"""Strict guardrails preset: json, strict_json, markdown, latex, pattern, zero_output, stall, repetition."""

JSON_ONLY_GUARDRAILS = Guardrails.json_only
"""JSON-only guardrails preset: json, strict_json, zero_output."""

MARKDOWN_ONLY_GUARDRAILS = Guardrails.markdown_only
"""Markdown-only guardrails preset: markdown, zero_output."""

LATEX_ONLY_GUARDRAILS = Guardrails.latex_only
"""LaTeX-only guardrails preset: latex, zero_output."""
