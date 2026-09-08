"""Contract tests against the installed OpenAI SDK (v2 or v3).

Every other test in this suite fakes provider chunks (see ``MockChunk`` in
test_adapters.py). These tests drive a real ``AsyncOpenAI`` client - including
its HTTP transport and SSE decoder - against a local server speaking the
chat.completions wire format, so they fail if an SDK major changes stream
types, chunk shapes, usage field names, or transport exception text.

No API key or network access is required.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

import l0
from l0.types import ErrorCategory, EventType
from tests.conftest import requires_openai_sdk

pytestmark = requires_openai_sdk

TOKENS = ("Hello", " from", " the", " SDK")
FULL_TEXT = "".join(TOKENS)


def _sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload)}\n\n"


def _chunk(delta: dict[str, Any] | None, usage: dict[str, int] | None = None) -> str:
    body: dict[str, Any] = {
        "id": "chatcmpl-test",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "gpt-4o-mini",
        "choices": (
            []
            if delta is None
            else [{"index": 0, "delta": delta, "finish_reason": None}]
        ),
    }
    if usage is not None:
        body["usage"] = usage
    return _sse(body)


TOOL_DELTA = {
    "tool_calls": [
        {
            "index": 0,
            "id": "call_1",
            "type": "function",
            "function": {"name": "get_weather", "arguments": '{"city":"Paris"}'},
        }
    ]
}


class _ChatCompletionsHandler(BaseHTTPRequestHandler):
    """Minimal chat.completions SSE endpoint.

    The ``x-l0-mode`` request header selects the response behaviour:
    ``text`` (default), ``tool``, or ``abort`` (socket killed mid-body on the
    first request only, so a retry can succeed).
    """

    protocol_version = "HTTP/1.1"
    attempts: dict[str, int] = {}

    def log_message(self, format: str, *args: Any) -> None:
        pass  # keep pytest output clean

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self.rfile.read(int(self.headers.get("content-length") or 0))
        mode = self.headers.get("x-l0-mode", "text")

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        attempt = _ChatCompletionsHandler.attempts.get(mode, 0)
        _ChatCompletionsHandler.attempts[mode] = attempt + 1
        abort_at = 2 if mode == "abort" and attempt == 0 else None

        for i, token in enumerate(TOKENS):
            if i == abort_at:
                # No terminating chunk, no [DONE]: a truncated response body.
                self.close_connection = True
                self.connection.close()
                return
            self._write(_chunk({"content": token}))

        if mode == "tool":
            self._write(_chunk(TOOL_DELTA))

        self._write(
            _chunk(
                None,
                usage={"prompt_tokens": 7, "completion_tokens": 4, "total_tokens": 11},
            )
        )
        self._write("data: [DONE]\n\n")
        self._write_chunked(b"")

    def _write(self, payload: str) -> None:
        self._write_chunked(payload.encode())

    def _write_chunked(self, data: bytes) -> None:
        try:
            self.wfile.write(f"{len(data):X}\r\n".encode())
            self.wfile.write(data + b"\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True


@pytest.fixture(scope="module")
def base_url() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ChatCompletionsHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}/v1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def client(base_url: str) -> Iterator[Any]:
    from openai import AsyncOpenAI

    _ChatCompletionsHandler.attempts.clear()
    yield AsyncOpenAI(api_key="test", base_url=base_url, max_retries=0, timeout=10.0)


def _factory(client: Any, mode: str = "text") -> Any:
    def create() -> Any:
        return client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            stream=True,
            stream_options={"include_usage": True},
            extra_headers={"x-l0-mode": mode},
        )

    return create


class TestSdkStreamAdaptation:
    async def test_sdk_stream_is_detected_as_openai(self, client: Any) -> None:
        """The SDK's stream object resolves to the OpenAI adapter."""
        stream = await _factory(client)()

        assert l0.Adapters.detect(stream).name == "openai"

        await stream.close()

    async def test_tokens_and_usage_are_extracted(self, client: Any) -> None:
        """Text deltas become tokens; SDK usage maps to L0 usage keys."""
        tokens: list[str] = []
        usage: dict[str, int] | None = None

        result = await l0.run(stream=_factory(client))
        async for event in result:
            if event.is_token and event.text:
                tokens.append(event.text)
            elif event.type == EventType.COMPLETE:
                usage = event.usage

        assert tokens == list(TOKENS)
        assert usage == {"input_tokens": 7, "output_tokens": 4}
        assert result.state.content == FULL_TEXT

    async def test_raw_chunks_are_sdk_models(self, client: Any) -> None:
        """Raw chunk passthrough preserves the SDK's own chunk type."""
        from openai.types.chat import ChatCompletionChunk

        result = await l0.run(stream=_factory(client))
        await result.read()

        chunks = result.raw()
        assert chunks
        assert all(isinstance(chunk, ChatCompletionChunk) for chunk in chunks)

    async def test_tool_call_deltas_are_reported(self, client: Any) -> None:
        """Tool call deltas from the SDK surface with name, id and arguments."""
        calls: list[tuple[str, str, dict[str, Any]]] = []

        result = await l0.run(
            stream=_factory(client, "tool"),
            on_tool_call=lambda name, id_, args: calls.append((name, id_, args)),
            buffer_tool_calls=True,
        )
        await result.read()

        assert calls == [("get_weather", "call_1", {"city": "Paris"})]

    async def test_wrapped_client_streams(self, client: Any) -> None:
        """l0.wrap() proxies chat.completions.create on an SDK client."""
        wrapped = l0.wrap(client)

        stream = await wrapped.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            stream=True,
        )

        assert await stream.read() == FULL_TEXT


class TestSdkTransportErrors:
    async def test_truncated_body_is_a_network_error(self, client: Any) -> None:
        """A killed response body raises an error L0 classifies as NETWORK.

        The SDK's HTTP stack (httpx in v2, httpx2 in v3) reports truncation via
        exception text, which is what L0's classifier matches on.
        """
        stream = await _factory(client, "abort")()

        with pytest.raises(Exception) as exc_info:  # noqa: PT011 - transport-specific
            async for _ in stream:
                pass

        assert l0.Error.categorize(exc_info.value) == ErrorCategory.NETWORK

    async def test_truncated_body_is_retried(self, client: Any) -> None:
        """Retry recovers the full response after a truncated attempt."""
        result = await l0.run(
            stream=_factory(client, "abort"),
            retry=l0.Retry(attempts=3, base_delay=0.01),
        )

        assert await result.read() == FULL_TEXT
        assert result.state.network_retry_count == 1
