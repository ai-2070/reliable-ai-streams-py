"""L0 state management."""

from __future__ import annotations

import time

from .types import State


def create_state() -> State:
    """Create fresh state."""
    return State()


def flush_content(state: State) -> None:
    """Materialize content buffer into content string. Call before reading state.content."""
    buf = state._content_buffer
    if buf:
        state.content = state.content + "".join(buf)
        buf.clear()


def update_checkpoint(state: State) -> None:
    """Save current content as checkpoint."""
    flush_content(state)
    state.checkpoint = state.content


def append_token(state: State, token: str) -> None:
    """Append token to content buffer and update timing."""
    now = time.time()
    if state.first_token_at is None:
        state.first_token_at = now
    state.last_token_at = now
    state._content_buffer.append(token)
    state.token_count += 1


def mark_completed(state: State) -> None:
    """Mark stream as completed and calculate duration."""
    flush_content(state)
    state.completed = True
    if state.first_token_at is not None:
        state.duration = (state.last_token_at or time.time()) - state.first_token_at
