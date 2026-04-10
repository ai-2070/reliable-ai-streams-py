"""L0 Drift Detection - Detect model derailment and anomalies.

Provides detection for various types of drift including:
- Tone shifts
- Meta commentary (AI self-references)
- Repetition
- Entropy spikes
- Format collapse
- Markdown collapse
- Excessive hedging
"""

from __future__ import annotations

import math
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

# ─────────────────────────────────────────────────────────────────────────────
# Pre-compiled regex patterns for performance (avoids re-compilation per check)
# ─────────────────────────────────────────────────────────────────────────────

# Meta commentary patterns (case-insensitive, checked on last 200 chars)
_META_COMMENTARY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"as an ai", re.IGNORECASE),
    re.compile(r"i'm an ai", re.IGNORECASE),
    re.compile(r"i am an ai", re.IGNORECASE),
    re.compile(r"i cannot actually", re.IGNORECASE),
    re.compile(r"i don't have personal", re.IGNORECASE),
    re.compile(r"i apologize, but i", re.IGNORECASE),
    re.compile(r"i'm sorry, but i", re.IGNORECASE),
    re.compile(r"let me explain", re.IGNORECASE),
    re.compile(r"to clarify", re.IGNORECASE),
    re.compile(r"in other words", re.IGNORECASE),
]

# Tone shift patterns
_FORMAL_PATTERN: re.Pattern[str] = re.compile(
    r"\b(therefore|thus|hence|moreover|furthermore|consequently)\b", re.IGNORECASE
)
_INFORMAL_PATTERN: re.Pattern[str] = re.compile(
    r"\b(gonna|wanna|yeah|yep|nope|ok|okay)\b", re.IGNORECASE
)

# Sentence split pattern
_SENTENCE_SPLIT_PATTERN: re.Pattern[str] = re.compile(r"[.!?]+")

# Format collapse patterns (checked on first 100 chars)
_FORMAT_COLLAPSE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"here is the .+?:", re.IGNORECASE),
    re.compile(r"here's the .+?:", re.IGNORECASE),
    re.compile(r"let me .+? for you", re.IGNORECASE),
    re.compile(r"i'll .+? for you", re.IGNORECASE),
    re.compile(r"here you go", re.IGNORECASE),
]

# Markdown patterns
_MARKDOWN_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"```"),
    re.compile(r"^#{1,6}\s", re.MULTILINE),
    re.compile(r"\*\*.*?\*\*"),
    re.compile(r"\[.*?\]\(.*?\)"),
]

# Hedging patterns (checked on first line)
_HEDGING_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^sure!?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^certainly!?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^of course!?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^absolutely!?\s*$", re.IGNORECASE | re.MULTILINE),
]

# Drift types that can be detected
DriftType = Literal[
    "tone_shift",
    "meta_commentary",
    "format_collapse",
    "repetition",
    "entropy_spike",
    "markdown_collapse",
    "hedging",
]


@dataclass
class DriftResult:
    """Result of drift detection check."""

    detected: bool
    """Whether drift was detected."""

    confidence: float
    """Confidence score (0-1)."""

    types: list[DriftType]
    """Types of drift detected."""

    details: str | None = None
    """Details about the drift."""


@dataclass
class DriftConfig:
    """Configuration for drift detection."""

    detect_tone_shift: bool = True
    """Enable tone shift detection."""

    detect_meta_commentary: bool = True
    """Enable meta commentary detection."""

    detect_repetition: bool = True
    """Enable repetition detection."""

    detect_entropy_spike: bool = True
    """Enable entropy spike detection."""

    repetition_threshold: int = 3
    """Repetition threshold (max repeated tokens)."""

    entropy_threshold: float = 2.5
    """Entropy threshold (standard deviations)."""

    entropy_window: int = 50
    """Window size for entropy calculation."""

    sliding_window_size: int = 500
    """Size of sliding window for content analysis (chars). Only the last N chars are analyzed."""


@dataclass
class _DriftHistory:
    """Internal history tracking for drift detection."""

    entropy: deque[float] = field(default_factory=lambda: deque(maxlen=50))
    tokens: deque[str] = field(default_factory=lambda: deque(maxlen=50))
    last_window: str = ""  # Store only the window, not full content


class DriftDetector:
    """Drift detector for detecting model derailment.

    Example:
        ```python
        from l0.drift import DriftDetector

        detector = DriftDetector()

        # Check content for drift
        result = detector.check(content, delta="latest token")

        if result.detected:
            print(f"Drift detected: {result.types}")
            print(f"Confidence: {result.confidence}")
        ```
    """

    def __init__(self, config: DriftConfig | None = None) -> None:
        """Create a drift detector.

        Args:
            config: Detection configuration (uses defaults if not provided)
        """
        self.config = config or DriftConfig()
        self._history = _DriftHistory(
            entropy=deque(maxlen=self.config.entropy_window),
            tokens=deque(maxlen=self.config.entropy_window),
        )

    def _get_window(self, content: str) -> str:
        """Get sliding window of content for analysis.

        Uses only the last N characters to avoid O(content_length) scanning.
        """
        window_size = self.config.sliding_window_size
        if len(content) <= window_size:
            return content
        return content[-window_size:]

    def check(self, content: str, delta: str | None = None) -> DriftResult:
        """Check content for drift.

        Args:
            content: Current content
            delta: Latest token/delta (optional)

        Returns:
            Drift detection result
        """
        types: list[DriftType] = []
        confidence = 0.0
        details: list[str] = []

        # Use sliding window for content analysis (O(window_size) instead of O(content_length))
        window = self._get_window(content)
        last_window = self._history.last_window

        # Update history (deque handles maxlen automatically)
        if delta:
            self._history.tokens.append(delta)

        # Check for meta commentary (on window only)
        if self.config.detect_meta_commentary:
            if self._detect_meta_commentary(window):
                types.append("meta_commentary")
                confidence = max(confidence, 0.9)
                details.append("Meta commentary detected")

        # Check for tone shift (on windows only)
        if self.config.detect_tone_shift:
            if self._detect_tone_shift(window, last_window):
                types.append("tone_shift")
                confidence = max(confidence, 0.7)
                details.append("Tone shift detected")

        # Check for repetition (on window only)
        if self.config.detect_repetition:
            if self._detect_repetition(window):
                types.append("repetition")
                confidence = max(confidence, 0.8)
                details.append("Excessive repetition detected")

        # Check for entropy spike
        if self.config.detect_entropy_spike and delta:
            entropy = self._calculate_entropy(delta)
            self._history.entropy.append(entropy)

            if self._detect_entropy_spike():
                types.append("entropy_spike")
                confidence = max(confidence, 0.6)
                details.append("Entropy spike detected")

        # Check for format collapse (already uses first 100 chars)
        if self._detect_format_collapse(content):
            types.append("format_collapse")
            confidence = max(confidence, 0.8)
            details.append("Format collapse detected")

        # Check for markdown collapse (on windows only)
        if self._detect_markdown_collapse(window, last_window):
            types.append("markdown_collapse")
            confidence = max(confidence, 0.7)
            details.append("Markdown formatting collapse detected")

        # Check for excessive hedging (already uses first line only)
        if self._detect_excessive_hedging(content):
            types.append("hedging")
            confidence = max(confidence, 0.5)
            details.append("Excessive hedging detected")

        # Update last window (store only the window, not full content)
        self._history.last_window = window

        return DriftResult(
            detected=len(types) > 0,
            confidence=confidence,
            types=types,
            details="; ".join(details) if details else None,
        )

    def _detect_meta_commentary(self, content: str) -> bool:
        """Detect meta commentary patterns using pre-compiled regexes."""
        # Check last 200 characters for meta commentary
        recent = content[-200:]
        return any(p.search(recent) for p in _META_COMMENTARY_PATTERNS)

    def _detect_tone_shift(self, content: str, previous_content: str) -> bool:
        """Detect tone shift between old and new content using pre-compiled regexes."""
        if not previous_content or len(previous_content) < 100:
            return False

        # Simple heuristic: check if formality suddenly changes
        recent_chunk = content[-200:]
        previous_chunk = previous_content[-200:]

        # Count formal markers using pre-compiled pattern
        recent_formal = len(_FORMAL_PATTERN.findall(recent_chunk))
        previous_formal = len(_FORMAL_PATTERN.findall(previous_chunk))

        # Count informal markers using pre-compiled pattern
        recent_informal = len(_INFORMAL_PATTERN.findall(recent_chunk))
        previous_informal = len(_INFORMAL_PATTERN.findall(previous_chunk))

        # Check for sudden shift
        formal_shift = abs(recent_formal - previous_formal) > 2
        informal_shift = abs(recent_informal - previous_informal) > 2

        return formal_shift or informal_shift

    def _detect_repetition(self, content: str) -> bool:
        """Detect excessive repetition using pre-compiled regex."""
        # Split into sentences using pre-compiled pattern
        sentences = [
            s.strip().lower()
            for s in _SENTENCE_SPLIT_PATTERN.split(content)
            if len(s.strip()) > 20
        ]

        if len(sentences) < 3:
            return False

        # Check for repeated sentences
        counts: dict[str, int] = {}
        for sentence in sentences:
            counts[sentence] = counts.get(sentence, 0) + 1

        # Check if any sentence repeats more than threshold
        for count in counts.values():
            if count >= self.config.repetition_threshold:
                return True

        # Check for repeated phrases (5+ words)
        words = content.lower().split()
        phrases: dict[str, int] = {}

        for i in range(len(words) - 4):
            phrase = " ".join(words[i : i + 5])
            phrases[phrase] = phrases.get(phrase, 0) + 1

        for count in phrases.values():
            if count >= self.config.repetition_threshold:
                return True

        return False

    def _calculate_entropy(self, text: str) -> float:
        """Calculate Shannon entropy of text."""
        if not text:
            return 0.0

        frequencies: dict[str, int] = {}
        for char in text:
            frequencies[char] = frequencies.get(char, 0) + 1

        entropy = 0.0
        length = len(text)

        for count in frequencies.values():
            probability = count / length
            entropy -= probability * math.log2(probability)

        return entropy

    def _detect_entropy_spike(self) -> bool:
        """Detect entropy spike."""
        if len(self._history.entropy) < 10:
            return False

        # Calculate mean and standard deviation
        mean = sum(self._history.entropy) / len(self._history.entropy)

        variance = sum((val - mean) ** 2 for val in self._history.entropy) / len(
            self._history.entropy
        )

        std_dev = math.sqrt(variance)

        if std_dev == 0:
            return False

        # Check if last value is significantly higher
        last = self._history.entropy[-1]
        return last > mean + self.config.entropy_threshold * std_dev

    def _detect_format_collapse(self, content: str) -> bool:
        """Detect format collapse using pre-compiled regexes."""
        # Only check beginning of content
        beginning = content[:100]
        return any(p.search(beginning) for p in _FORMAT_COLLAPSE_PATTERNS)

    def _detect_markdown_collapse(self, content: str, previous_content: str) -> bool:
        """Detect markdown to plaintext collapse using pre-compiled regexes."""
        if not previous_content or len(previous_content) < 100:
            return False

        recent = content[-200:]
        previous = previous_content[-200:]

        recent_markdown = 0
        previous_markdown = 0

        # Count markdown elements using pre-compiled patterns
        for pattern in _MARKDOWN_PATTERNS:
            recent_markdown += len(pattern.findall(recent))
            previous_markdown += len(pattern.findall(previous))

        # Check if markdown suddenly drops
        return previous_markdown > 3 and recent_markdown == 0

    def _detect_excessive_hedging(self, content: str) -> bool:
        """Detect excessive hedging at start using pre-compiled regexes."""
        first_line = content.strip().split("\n")[0] if content.strip() else ""
        return any(p.search(first_line) for p in _HEDGING_PATTERNS)

    def reset(self) -> None:
        """Reset detector state."""
        self._history = _DriftHistory(
            entropy=deque(maxlen=self.config.entropy_window),
            tokens=deque(maxlen=self.config.entropy_window),
        )

    def get_history(self) -> dict[str, Any]:
        """Get detection history."""
        return {
            "entropy": list(self._history.entropy),
            "tokens": list(self._history.tokens),
            "last_content": self._history.last_window,
        }


def create_drift_detector(config: DriftConfig | None = None) -> DriftDetector:
    """Create a drift detector with configuration.

    Args:
        config: Detection configuration

    Returns:
        Configured drift detector
    """
    return DriftDetector(config)


def check_drift(content: str) -> DriftResult:
    """Quick check for drift without creating detector instance.

    Args:
        content: Content to check

    Returns:
        Drift detection result
    """
    detector = DriftDetector()
    return detector.check(content)


# ─────────────────────────────────────────────────────────────────────────────
# Scoped API
# ─────────────────────────────────────────────────────────────────────────────


class Drift:
    """Scoped API for drift detection utilities.

    Provides detection for various types of model derailment including
    tone shifts, meta commentary, repetition, entropy spikes, and format collapse.

    Usage:
        ```python
        from l0 import Drift

        # Quick check for drift
        result = Drift.check("Some content to check")
        if result.detected:
            print(f"Drift types: {result.types}")

        # Create a detector for streaming
        detector = Drift.create_detector()
        for token in stream:
            result = detector.check(content, delta=token)
            if result.detected:
                handle_drift(result)
        ```
    """

    # Re-export types for convenience
    Result = DriftResult
    Config = DriftConfig
    Detector = DriftDetector

    @staticmethod
    def check(content: str) -> DriftResult:
        """Quick check for drift without creating detector instance.

        Args:
            content: Content to check

        Returns:
            Drift detection result
        """
        return check_drift(content)

    @staticmethod
    def create_detector(config: DriftConfig | None = None) -> DriftDetector:
        """Create a drift detector with configuration.

        Args:
            config: Detection configuration

        Returns:
            Configured drift detector
        """
        return create_drift_detector(config)
