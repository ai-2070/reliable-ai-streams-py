"""Canonical Specification Tests for L0 Runtime (Python)

These tests validate the L0 runtime against the canonical specification
defined in fixtures/canonical-spec.json. This ensures consistency between
TypeScript and Python implementations.

Tests cover:
- L0Error structure and to_json() format
- Error code to category mapping
- Observability event structure and field schemas
- Callback parameter schemas
- Network error classification
- Lifecycle invariants
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from l0.errors import Error, ErrorCategory, ErrorCode, ErrorContext
from l0.events import ObservabilityEventType


def load_spec() -> dict[str, Any]:
    fixture_path = Path(__file__).parent / "fixtures" / "canonical-spec.json"
    with open(fixture_path) as f:
        return json.load(f)


SPEC = load_spec()


# ============================================================================
# Helper Functions
# ============================================================================


def validate_exact_parameters(
    params: list[dict[str, Any]],
    expected_params: list[dict[str, str]],
) -> None:
    """Validates that a callback has exactly the specified parameters."""
    assert len(params) == len(expected_params), (
        f"Expected {len(expected_params)} params, got {len(params)}"
    )

    for expected in expected_params:
        param = next((p for p in params if p["name"] == expected["name"]), None)
        assert param is not None, f"Missing parameter: {expected['name']}"
        assert param["type"] == expected["type"], (
            f"Parameter {expected['name']} type mismatch: "
            f"expected {expected['type']}, got {param['type']}"
        )

    expected_names = [p["name"] for p in expected_params]
    for param in params:
        assert param["name"] in expected_names, f"Unexpected parameter: {param['name']}"


def validate_exact_fields(
    fields: list[dict[str, Any]],
    expected_fields: list[dict[str, Any]],
) -> None:
    """Validates that an event has exactly the specified fields."""
    assert len(fields) == len(expected_fields), (
        f"Expected {len(expected_fields)} fields, got {len(fields)}"
    )

    for expected in expected_fields:
        field = next((f for f in fields if f["name"] == expected["name"]), None)
        assert field is not None, f"Missing field: {expected['name']}"
        assert field["type"] == expected["type"], (
            f"Field {expected['name']} type mismatch: "
            f"expected {expected['type']}, got {field['type']}"
        )
        if "required" in expected:
            assert field["required"] == expected["required"], (
                f"Field {expected['name']} required mismatch: "
                f"expected {expected['required']}, got {field['required']}"
            )

    expected_names = [f["name"] for f in expected_fields]
    for field in fields:
        assert field["name"] in expected_names, f"Unexpected field: {field['name']}"


# ============================================================================
# L0Error Tests
# ============================================================================


class TestL0ErrorToJSON:
    """Tests for Error.to_json() canonical format."""

    def test_returns_all_required_fields(self) -> None:
        error = Error(
            "Test error",
            ErrorContext(
                code=ErrorCode.STREAM_ABORTED,
                checkpoint="checkpoint-content",
                token_count=10,
                model_retry_count=2,
                network_retry_count=1,
                fallback_index=0,
                metadata={"violation": {"rule": "test"}},
                context={"requestId": "req-123"},
            ),
        )

        result = error.to_json()

        # Verify all fields from canonical spec (camelCase)
        assert result["name"] == "Error"
        assert result["code"] == "STREAM_ABORTED"
        assert "category" in result
        assert result["message"] == "Test error"
        assert "timestamp" in result
        assert result["hasCheckpoint"] is True
        assert result["checkpoint"] == "checkpoint-content"
        assert result["tokenCount"] == 10
        assert result["modelRetryCount"] == 2
        assert result["networkRetryCount"] == 1
        assert result["fallbackIndex"] == 0
        assert "metadata" in result
        assert "context" in result

    def test_includes_metadata_for_internal_state(self) -> None:
        error = Error(
            "Guardrail failed",
            ErrorContext(
                code=ErrorCode.GUARDRAIL_VIOLATION,
                metadata={
                    "violation": {
                        "rule": "no-pii",
                        "severity": "error",
                        "message": "PII detected",
                    }
                },
            ),
        )

        result = error.to_json()
        assert result["metadata"] == {
            "violation": {
                "rule": "no-pii",
                "severity": "error",
                "message": "PII detected",
            }
        }

    def test_includes_context_for_user_provided_data(self) -> None:
        error = Error(
            "Network error",
            ErrorContext(
                code=ErrorCode.NETWORK_ERROR,
                context={
                    "requestId": "req-456",
                    "userId": "user-789",
                    "nested": {"traceId": "trace-abc"},
                },
            ),
        )

        result = error.to_json()
        assert result["context"] == {
            "requestId": "req-456",
            "userId": "user-789",
            "nested": {"traceId": "trace-abc"},
        }

    def test_handles_undefined_optional_fields(self) -> None:
        error = Error(
            "Minimal error",
            ErrorContext(code=ErrorCode.INVALID_STREAM),
        )

        result = error.to_json()
        assert result["checkpoint"] is None
        assert result["tokenCount"] is None
        assert result["modelRetryCount"] is None
        assert result["networkRetryCount"] is None
        assert result["fallbackIndex"] is None
        assert result["metadata"] is None
        assert result["context"] is None

    def test_computes_has_checkpoint_correctly(self) -> None:
        # No checkpoint
        error1 = Error("No checkpoint", ErrorContext(code=ErrorCode.STREAM_ABORTED))
        assert error1.to_json()["hasCheckpoint"] is False

        # Empty checkpoint
        error2 = Error(
            "Empty checkpoint",
            ErrorContext(code=ErrorCode.STREAM_ABORTED, checkpoint=""),
        )
        assert error2.to_json()["hasCheckpoint"] is False

        # Valid checkpoint
        error3 = Error(
            "Has checkpoint",
            ErrorContext(code=ErrorCode.STREAM_ABORTED, checkpoint="content"),
        )
        assert error3.to_json()["hasCheckpoint"] is True

    def test_to_json_field_names_are_camel_case(self) -> None:
        """Verify all field names use camelCase to match TypeScript."""
        error = Error(
            "Test",
            ErrorContext(
                code=ErrorCode.STREAM_ABORTED,
                checkpoint="x",
                token_count=1,
                model_retry_count=1,
                network_retry_count=1,
                fallback_index=0,
            ),
        )

        result = error.to_json()
        keys = set(result.keys())

        # Should have camelCase keys
        assert "hasCheckpoint" in keys
        assert "tokenCount" in keys
        assert "modelRetryCount" in keys
        assert "networkRetryCount" in keys
        assert "fallbackIndex" in keys

        # Should NOT have snake_case keys
        assert "has_checkpoint" not in keys
        assert "token_count" not in keys
        assert "model_retry_count" not in keys
        assert "network_retry_count" not in keys
        assert "fallback_index" not in keys


# ============================================================================
# Error Code Tests
# ============================================================================


class TestErrorCodeToCategoryMapping:
    """Tests for error code to category mapping."""

    def test_maps_network_error_to_network_category(self) -> None:
        error = Error("test", ErrorContext(code=ErrorCode.NETWORK_ERROR))
        assert error.category == ErrorCategory.NETWORK

    def test_maps_timeout_codes_to_transient_category(self) -> None:
        error1 = Error("test", ErrorContext(code=ErrorCode.INITIAL_TOKEN_TIMEOUT))
        assert error1.category == ErrorCategory.TRANSIENT

        error2 = Error("test", ErrorContext(code=ErrorCode.INTER_TOKEN_TIMEOUT))
        assert error2.category == ErrorCategory.TRANSIENT

    def test_maps_content_quality_codes_to_content_category(self) -> None:
        codes = [
            ErrorCode.GUARDRAIL_VIOLATION,
            ErrorCode.FATAL_GUARDRAIL_VIOLATION,
            ErrorCode.DRIFT_DETECTED,
            ErrorCode.ZERO_OUTPUT,
        ]
        for code in codes:
            error = Error("test", ErrorContext(code=code))
            assert error.category == ErrorCategory.CONTENT, (
                f"{code} should map to CONTENT"
            )

    def test_maps_internal_codes_to_internal_category(self) -> None:
        codes = [
            ErrorCode.INVALID_STREAM,
            ErrorCode.ADAPTER_NOT_FOUND,
            ErrorCode.FEATURE_NOT_ENABLED,
        ]
        for code in codes:
            error = Error("test", ErrorContext(code=code))
            assert error.category == ErrorCategory.INTERNAL, (
                f"{code} should map to INTERNAL"
            )

    def test_maps_provider_codes_to_provider_category(self) -> None:
        codes = [
            ErrorCode.STREAM_ABORTED,
            ErrorCode.ALL_STREAMS_EXHAUSTED,
        ]
        for code in codes:
            error = Error("test", ErrorContext(code=code))
            assert error.category == ErrorCategory.PROVIDER, (
                f"{code} should map to PROVIDER"
            )


class TestAllErrorCodesExist:
    """Tests that all spec error codes exist in Python."""

    @pytest.fixture
    def spec_error_codes(self) -> list[str]:
        return list(SPEC["errorHandling"]["L0ErrorCodes"]["values"].keys())

    def test_all_spec_error_codes_exist(self, spec_error_codes: list[str]) -> None:
        for code in spec_error_codes:
            assert hasattr(ErrorCode, code), f"ErrorCode.{code} should exist"


# ============================================================================
# Network Error Classification Tests
# ============================================================================


from l0.errors import (
    NetworkErrorType,
    analyze_network_error,
    is_network_error,
)


class TestNetworkErrorClassification:
    """Tests for network error classification from canonical spec."""

    @pytest.fixture
    def network_error_types(self) -> dict[str, Any]:
        return SPEC["networkErrorTypes"]["types"]


class TestConnectionDroppedDetection(TestNetworkErrorClassification):
    def test_detection_patterns(self, network_error_types: dict[str, Any]) -> None:
        patterns = network_error_types["connection_dropped"]["detection"]
        for pattern in patterns:
            error = Exception(f"Network: {pattern}")
            analysis = analyze_network_error(error)
            assert analysis.type == NetworkErrorType.CONNECTION_DROPPED, (
                f'Expected CONNECTION_DROPPED for "{pattern}", got {analysis.type}'
            )
            assert analysis.retryable is True
            assert analysis.counts_toward_limit is False


class TestDnsErrorDetection(TestNetworkErrorClassification):
    def test_detection_patterns(self, network_error_types: dict[str, Any]) -> None:
        patterns = network_error_types["dns_error"]["detection"]
        for pattern in patterns:
            error = Exception(f"DNS error: {pattern}")
            analysis = analyze_network_error(error)
            assert analysis.type == NetworkErrorType.DNS_ERROR, (
                f'Expected DNS_ERROR for "{pattern}", got {analysis.type}'
            )
            assert analysis.retryable is True


class TestSslErrorDetection(TestNetworkErrorClassification):
    def test_detection_patterns_non_retryable(
        self, network_error_types: dict[str, Any]
    ) -> None:
        patterns = network_error_types["ssl_error"]["detection"]
        for pattern in patterns:
            error = Exception(f"SSL error: {pattern}")
            analysis = analyze_network_error(error)
            assert analysis.type == NetworkErrorType.SSL_ERROR, (
                f'Expected SSL_ERROR for "{pattern}", got {analysis.type}'
            )
            assert analysis.retryable is False


class TestTimeoutDetection(TestNetworkErrorClassification):
    def test_detection_patterns(self, network_error_types: dict[str, Any]) -> None:
        patterns = network_error_types["timeout"]["detection"]
        for pattern in patterns:
            error = Exception(f"Request {pattern}")
            analysis = analyze_network_error(error)
            assert analysis.type == NetworkErrorType.TIMEOUT, (
                f'Expected TIMEOUT for "{pattern}", got {analysis.type}'
            )
            assert analysis.retryable is True


class TestFetchErrorDetection(TestNetworkErrorClassification):
    def test_detect_failed_to_fetch(self) -> None:
        error = TypeError("Failed to fetch")
        analysis = analyze_network_error(error)
        assert analysis.type == NetworkErrorType.FETCH_ERROR
        assert analysis.retryable is True

    def test_detect_network_request_failed(self) -> None:
        error = TypeError("Network request failed")
        analysis = analyze_network_error(error)
        assert analysis.type == NetworkErrorType.FETCH_ERROR
        assert analysis.retryable is True


class TestIsNetworkErrorUtility:
    def test_returns_true_for_network_errors(self) -> None:
        assert is_network_error(Exception("connection dropped")) is True
        assert is_network_error(Exception("econnreset")) is True
        assert is_network_error(Exception("dns lookup failed")) is True
        assert is_network_error(Exception("request timeout")) is True

    def test_returns_false_for_non_network_errors(self) -> None:
        assert is_network_error(Exception("Invalid JSON")) is False
        assert is_network_error(Exception("Schema validation failed")) is False


# ============================================================================
# Observability Events Tests
# ============================================================================


class TestObservabilityEvents:
    """Tests for observability event types."""

    @pytest.fixture
    def spec_events(self) -> list[str]:
        return list(SPEC["monitoring"]["observabilityEvents"]["events"].keys())

    def test_all_spec_events_exist(self, spec_events: list[str]) -> None:
        for event in spec_events:
            assert hasattr(ObservabilityEventType, event), (
                f"ObservabilityEventType.{event} should exist"
            )

    def test_event_values_match_keys(self) -> None:
        event_types = [
            "SESSION_START",
            "ATTEMPT_START",
            "FALLBACK_START",
            "RETRY_ATTEMPT",
            "ERROR",
            "COMPLETE",
            "CHECKPOINT_SAVED",
            "RESUME_START",
            "ABORT_COMPLETED",
            "GUARDRAIL_RULE_RESULT",
            "TIMEOUT_TRIGGERED",
        ]
        for evt in event_types:
            assert getattr(ObservabilityEventType, evt).value == evt


# ============================================================================
# Callback Specification Tests
# ============================================================================


class TestCallbackTriggeredBy:
    """Tests that callbacks document correct triggeredBy events."""

    @pytest.fixture
    def callbacks(self) -> dict[str, Any]:
        return SPEC["callbacks"]["callbacks"]

    @pytest.fixture
    def valid_event_types(self) -> list[str]:
        return list(SPEC["monitoring"]["observabilityEvents"]["events"].keys())

    def test_on_start_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "SESSION_START" in callbacks["onStart"]["triggeredBy"]
        assert "ATTEMPT_START" in callbacks["onStart"]["triggeredBy"]
        assert "FALLBACK_START" in callbacks["onStart"]["triggeredBy"]

    def test_on_complete_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "COMPLETE" in callbacks["onComplete"]["triggeredBy"]

    def test_on_error_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "ERROR" in callbacks["onError"]["triggeredBy"]

    def test_on_retry_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "RETRY_ATTEMPT" in callbacks["onRetry"]["triggeredBy"]

    def test_on_fallback_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "FALLBACK_START" in callbacks["onFallback"]["triggeredBy"]

    def test_on_checkpoint_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "CHECKPOINT_SAVED" in callbacks["onCheckpoint"]["triggeredBy"]

    def test_on_resume_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "RESUME_START" in callbacks["onResume"]["triggeredBy"]

    def test_on_abort_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "ABORT_COMPLETED" in callbacks["onAbort"]["triggeredBy"]

    def test_on_timeout_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "TIMEOUT_TRIGGERED" in callbacks["onTimeout"]["triggeredBy"]

    def test_on_violation_triggers(self, callbacks: dict[str, Any]) -> None:
        assert len(callbacks["onViolation"]["triggeredBy"]) > 0

    def test_on_drift_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "DRIFT_CHECK_RESULT" in callbacks["onDrift"]["triggeredBy"]

    def test_on_tool_call_triggers(self, callbacks: dict[str, Any]) -> None:
        assert "TOOL_REQUESTED" in callbacks["onToolCall"]["triggeredBy"]


class TestTriggeredByReferencesValidEvents:
    """Tests that all triggeredBy references point to valid observability events."""

    @pytest.fixture
    def callbacks(self) -> dict[str, Any]:
        return SPEC["callbacks"]["callbacks"]

    @pytest.fixture
    def valid_event_types(self) -> list[str]:
        return list(SPEC["monitoring"]["observabilityEvents"]["events"].keys())

    def test_all_triggered_by_reference_valid_events(
        self,
        callbacks: dict[str, Any],
        valid_event_types: list[str],
    ) -> None:
        """Generic test that validates all triggeredBy references."""
        for callback_name, callback in callbacks.items():
            triggered_by = callback.get("triggeredBy", [])
            assert triggered_by, f"{callback_name} should have triggeredBy"
            assert isinstance(triggered_by, list), (
                f"{callback_name}.triggeredBy should be a list"
            )

            for trigger in triggered_by:
                # Handle special case like "GUARDRAIL_RULE_RESULT (when passed=false)"
                base_trigger = trigger.split(" ")[0]
                assert base_trigger in valid_event_types, (
                    f"{callback_name}.triggeredBy contains invalid event: {trigger}"
                )


# Generate individual tests for each callback
_CALLBACKS = SPEC["callbacks"]["callbacks"]
_VALID_EVENTS = list(SPEC["monitoring"]["observabilityEvents"]["events"].keys())


@pytest.mark.parametrize("callback_name", list(_CALLBACKS.keys()))
def test_callback_triggered_by_valid_events(callback_name: str) -> None:
    """Each callback's triggeredBy should reference valid events."""
    callback = _CALLBACKS[callback_name]
    triggered_by = callback.get("triggeredBy", [])

    assert triggered_by, f"{callback_name} should have triggeredBy"
    assert isinstance(triggered_by, list), f"{callback_name}.triggeredBy should be list"
    assert len(triggered_by) > 0, f"{callback_name}.triggeredBy should not be empty"

    for trigger in triggered_by:
        base_trigger = trigger.split(" ")[0]
        assert base_trigger in _VALID_EVENTS, (
            f"{callback_name}.triggeredBy contains invalid event: {trigger}"
        )


# ============================================================================
# Callback Parameter Schema Tests
# ============================================================================


class TestCallbackParameterSchemas:
    """Tests for callback parameter schemas."""

    @pytest.fixture
    def callbacks(self) -> dict[str, Any]:
        return SPEC["callbacks"]["callbacks"]

    def test_all_callbacks_have_parameters(self, callbacks: dict[str, Any]) -> None:
        for name, callback in callbacks.items():
            assert "parameters" in callback, f"{name} should have parameters"
            assert isinstance(callback["parameters"], list), (
                f"{name}.parameters should be a list"
            )
            assert len(callback["parameters"]) > 0, (
                f"{name} should have at least one parameter"
            )


class TestOnStartParameterSchema:
    """Tests for onStart callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onStart"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "attempt", "type": "number"},
                {"name": "isRetry", "type": "boolean"},
                {"name": "isFallback", "type": "boolean"},
            ],
        )

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnCompleteParameterSchema:
    """Tests for onComplete callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onComplete"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "state", "type": "L0State"},
            ],
        )

    def test_state_has_required_shape(self, params: list[dict[str, Any]]) -> None:
        state_param = next(p for p in params if p["name"] == "state")
        assert "shape" in state_param
        assert "content" in state_param["shape"]
        assert "tokenCount" in state_param["shape"]
        assert "checkpoint" in state_param["shape"]


class TestOnErrorParameterSchema:
    """Tests for onError callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onError"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "error", "type": "L0Error"},
                {"name": "willRetry", "type": "boolean"},
                {"name": "willFallback", "type": "boolean"},
            ],
        )

    def test_error_has_required_shape(self, params: list[dict[str, Any]]) -> None:
        error_param = next(p for p in params if p["name"] == "error")
        assert "shape" in error_param
        assert "message" in error_param["shape"]
        assert "code" in error_param["shape"]
        assert "category" in error_param["shape"]

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnRetryParameterSchema:
    """Tests for onRetry callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onRetry"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "attempt", "type": "number"},
                {"name": "reason", "type": "string"},
            ],
        )

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnFallbackParameterSchema:
    """Tests for onFallback callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onFallback"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "index", "type": "number"},
                {"name": "reason", "type": "string"},
            ],
        )

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnCheckpointParameterSchema:
    """Tests for onCheckpoint callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onCheckpoint"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "checkpoint", "type": "string"},
                {"name": "tokenCount", "type": "number"},
            ],
        )

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnResumeParameterSchema:
    """Tests for onResume callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onResume"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "checkpoint", "type": "string"},
                {"name": "tokenCount", "type": "number"},
            ],
        )

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnAbortParameterSchema:
    """Tests for onAbort callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onAbort"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "tokenCount", "type": "number"},
                {"name": "contentLength", "type": "number"},
            ],
        )

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnTimeoutParameterSchema:
    """Tests for onTimeout callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onTimeout"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "type", "type": "string"},
                {"name": "elapsedMs", "type": "number"},
            ],
        )

    def test_type_has_enum_constraint(self, params: list[dict[str, Any]]) -> None:
        type_param = next(p for p in params if p["name"] == "type")
        assert "enum" in type_param
        assert "initial" in type_param["enum"]
        assert "inter" in type_param["enum"]
        assert len(type_param["enum"]) == 2

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


class TestOnViolationParameterSchema:
    """Tests for onViolation callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onViolation"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "violation", "type": "GuardrailViolation"},
            ],
        )

    def test_violation_has_required_shape(self, params: list[dict[str, Any]]) -> None:
        violation_param = next(p for p in params if p["name"] == "violation")
        assert "shape" in violation_param
        assert "ruleId" in violation_param["shape"]
        assert "message" in violation_param["shape"]
        assert "severity" in violation_param["shape"]


class TestOnDriftParameterSchema:
    """Tests for onDrift callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onDrift"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "types", "type": "string[]"},
                {"name": "confidence", "type": "number"},
            ],
        )

    def test_types_required_confidence_optional(
        self, params: list[dict[str, Any]]
    ) -> None:
        types_param = next(p for p in params if p["name"] == "types")
        confidence_param = next(p for p in params if p["name"] == "confidence")
        assert types_param["required"] is True
        assert confidence_param["required"] is False


class TestOnToolCallParameterSchema:
    """Tests for onToolCall callback parameter schema."""

    @pytest.fixture
    def params(self) -> list[dict[str, Any]]:
        return SPEC["callbacks"]["callbacks"]["onToolCall"]["parameters"]

    def test_has_exact_parameters(self, params: list[dict[str, Any]]) -> None:
        validate_exact_parameters(
            params,
            [
                {"name": "toolName", "type": "string"},
                {"name": "toolCallId", "type": "string"},
                {"name": "args", "type": "Record<string, unknown>"},
            ],
        )

    def test_all_parameters_required(self, params: list[dict[str, Any]]) -> None:
        for param in params:
            assert param["required"] is True, f"{param['name']} should be required"


# ============================================================================
# Observability Event Field Schema Tests
# ============================================================================


class TestObservabilityEventBaseShape:
    """Tests for observability event base shape."""

    @pytest.fixture
    def base_shape(self) -> dict[str, Any]:
        return SPEC["monitoring"]["observabilityEvents"]["baseShape"]

    def test_has_required_base_fields(self, base_shape: dict[str, Any]) -> None:
        assert base_shape["type"]["required"] is True
        assert base_shape["type"]["type"] == "EventType"

        assert base_shape["ts"]["required"] is True
        assert base_shape["ts"]["type"] == "number"

        assert base_shape["streamId"]["required"] is True
        assert base_shape["streamId"]["type"] == "string"

    def test_has_optional_context_field(self, base_shape: dict[str, Any]) -> None:
        assert base_shape["context"]["required"] is False
        assert base_shape["context"]["type"] == "Record<string, unknown>"


class TestAllEventsHaveFieldsArray:
    """Tests that all events have fields arrays."""

    @pytest.fixture
    def events(self) -> dict[str, Any]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]

    def test_all_events_have_fields(self, events: dict[str, Any]) -> None:
        for name, event in events.items():
            assert "fields" in event, f"{name} should have fields"
            assert isinstance(event["fields"], list), f"{name}.fields should be a list"


# Session Events
class TestSessionStartFieldSchema:
    """Tests for SESSION_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["SESSION_START"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "attempt", "type": "number", "required": True},
                {"name": "isRetry", "type": "boolean", "required": True},
                {"name": "isFallback", "type": "boolean", "required": True},
            ],
        )


class TestStreamInitFieldSchema:
    """Tests for STREAM_INIT event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["STREAM_INIT"][
            "fields"
        ]

    def test_has_no_additional_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(fields, [])


class TestStreamReadyFieldSchema:
    """Tests for STREAM_READY event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["STREAM_READY"][
            "fields"
        ]

    def test_has_no_additional_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(fields, [])


class TestSessionEndFieldSchema:
    """Tests for SESSION_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["SESSION_END"][
            "fields"
        ]

    def test_has_no_additional_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(fields, [])


class TestSessionSummaryFieldSchema:
    """Tests for SESSION_SUMMARY event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["SESSION_SUMMARY"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "tokenCount", "type": "number", "required": True},
                {"name": "startTs", "type": "number", "required": True},
                {"name": "endTs", "type": "number", "required": True},
                {"name": "driftDetected", "type": "boolean", "required": True},
                {"name": "guardrailViolations", "type": "number", "required": True},
                {"name": "fallbackDepth", "type": "number", "required": True},
                {"name": "retryCount", "type": "number", "required": True},
                {"name": "checkpointsCreated", "type": "number", "required": True},
            ],
        )


# Adapter Events
class TestAdapterWrapStartFieldSchema:
    """Tests for ADAPTER_WRAP_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "ADAPTER_WRAP_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "streamType", "type": "string", "required": True},
                {"name": "adapterId", "type": "string", "required": False},
            ],
        )


class TestAdapterDetectedFieldSchema:
    """Tests for ADAPTER_DETECTED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["ADAPTER_DETECTED"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "adapterId", "type": "string", "required": True},
            ],
        )


class TestAdapterWrapEndFieldSchema:
    """Tests for ADAPTER_WRAP_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["ADAPTER_WRAP_END"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "adapterId", "type": "string", "required": True},
            ],
        )


# Timeout Events
class TestTimeoutStartFieldSchema:
    """Tests for TIMEOUT_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TIMEOUT_START"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "timeoutType", "type": "string", "required": True},
                {"name": "configuredMs", "type": "number", "required": True},
            ],
        )

    def test_timeout_type_has_enum(self, fields: list[dict[str, Any]]) -> None:
        field = next(f for f in fields if f["name"] == "timeoutType")
        assert "enum" in field
        assert "initial" in field["enum"]
        assert "inter" in field["enum"]
        assert len(field["enum"]) == 2


class TestTimeoutResetFieldSchema:
    """Tests for TIMEOUT_RESET event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TIMEOUT_RESET"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "timeoutType", "type": "string", "required": True},
                {"name": "configuredMs", "type": "number", "required": True},
                {"name": "tokenIndex", "type": "number", "required": True},
            ],
        )

    def test_timeout_type_has_enum(self, fields: list[dict[str, Any]]) -> None:
        field = next(f for f in fields if f["name"] == "timeoutType")
        assert "enum" in field
        assert "initial" in field["enum"]
        assert "inter" in field["enum"]
        assert len(field["enum"]) == 2


class TestTimeoutTriggeredFieldSchema:
    """Tests for TIMEOUT_TRIGGERED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TIMEOUT_TRIGGERED"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "timeoutType", "type": "string", "required": True},
                {"name": "elapsedMs", "type": "number", "required": True},
                {"name": "configuredMs", "type": "number", "required": True},
            ],
        )

    def test_timeout_type_has_enum(self, fields: list[dict[str, Any]]) -> None:
        field = next(f for f in fields if f["name"] == "timeoutType")
        assert "enum" in field
        assert "initial" in field["enum"]
        assert "inter" in field["enum"]


# Network Events
class TestNetworkErrorFieldSchema:
    """Tests for NETWORK_ERROR event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["NETWORK_ERROR"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "error", "type": "string", "required": True},
                {"name": "code", "type": "string", "required": False},
                {"name": "retryable", "type": "boolean", "required": True},
            ],
        )


class TestNetworkRecoveryFieldSchema:
    """Tests for NETWORK_RECOVERY event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["NETWORK_RECOVERY"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "attemptCount", "type": "number", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


class TestConnectionDroppedFieldSchema:
    """Tests for CONNECTION_DROPPED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "CONNECTION_DROPPED"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "reason", "type": "string", "required": True},
            ],
        )


class TestConnectionRestoredFieldSchema:
    """Tests for CONNECTION_RESTORED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "CONNECTION_RESTORED"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


# Abort Events
class TestAbortRequestedFieldSchema:
    """Tests for ABORT_REQUESTED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["ABORT_REQUESTED"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "source", "type": "string", "required": True},
            ],
        )

    def test_source_has_enum(self, fields: list[dict[str, Any]]) -> None:
        field = next(f for f in fields if f["name"] == "source")
        assert "enum" in field
        assert "user" in field["enum"]
        assert "timeout" in field["enum"]
        assert "error" in field["enum"]


class TestAbortCompletedFieldSchema:
    """Tests for ABORT_COMPLETED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["ABORT_COMPLETED"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "tokenCount", "type": "number", "required": True},
                {"name": "contentLength", "type": "number", "required": True},
            ],
        )


# Tool Events
class TestToolRequestedFieldSchema:
    """Tests for TOOL_REQUESTED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TOOL_REQUESTED"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "toolName", "type": "string", "required": True},
                {"name": "toolCallId", "type": "string", "required": True},
                {
                    "name": "arguments",
                    "type": "Record<string, unknown>",
                    "required": True,
                },
            ],
        )


class TestToolStartFieldSchema:
    """Tests for TOOL_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TOOL_START"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "toolCallId", "type": "string", "required": True},
                {"name": "toolName", "type": "string", "required": True},
            ],
        )


class TestToolResultFieldSchema:
    """Tests for TOOL_RESULT event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TOOL_RESULT"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "toolCallId", "type": "string", "required": True},
                {"name": "result", "type": "unknown", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


class TestToolErrorFieldSchema:
    """Tests for TOOL_ERROR event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TOOL_ERROR"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "toolCallId", "type": "string", "required": True},
                {"name": "error", "type": "string", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


class TestToolCompletedFieldSchema:
    """Tests for TOOL_COMPLETED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["TOOL_COMPLETED"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "toolCallId", "type": "string", "required": True},
                {"name": "status", "type": "string", "required": True},
            ],
        )

    def test_status_has_enum(self, fields: list[dict[str, Any]]) -> None:
        field = next(f for f in fields if f["name"] == "status")
        assert "enum" in field
        assert "success" in field["enum"]
        assert "error" in field["enum"]


# Guardrail Events
class TestGuardrailPhaseStartFieldSchema:
    """Tests for GUARDRAIL_PHASE_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "GUARDRAIL_PHASE_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "phase", "type": "string", "required": True},
                {"name": "ruleCount", "type": "number", "required": True},
            ],
        )

    def test_phase_has_enum(self, fields: list[dict[str, Any]]) -> None:
        field = next(f for f in fields if f["name"] == "phase")
        assert "enum" in field
        assert "pre" in field["enum"]
        assert "post" in field["enum"]


class TestGuardrailRuleStartFieldSchema:
    """Tests for GUARDRAIL_RULE_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "GUARDRAIL_RULE_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "index", "type": "number", "required": True},
                {"name": "ruleId", "type": "string", "required": True},
                {"name": "callbackId", "type": "string", "required": True},
            ],
        )


class TestGuardrailRuleResultFieldSchema:
    """Tests for GUARDRAIL_RULE_RESULT event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "GUARDRAIL_RULE_RESULT"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "index", "type": "number", "required": True},
                {"name": "ruleId", "type": "string", "required": True},
                {"name": "passed", "type": "boolean", "required": True},
                {"name": "violation", "type": "GuardrailViolation", "required": False},
            ],
        )


class TestGuardrailRuleEndFieldSchema:
    """Tests for GUARDRAIL_RULE_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "GUARDRAIL_RULE_END"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "index", "type": "number", "required": True},
                {"name": "ruleId", "type": "string", "required": True},
                {"name": "passed", "type": "boolean", "required": True},
                {"name": "callbackId", "type": "string", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


class TestGuardrailPhaseEndFieldSchema:
    """Tests for GUARDRAIL_PHASE_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "GUARDRAIL_PHASE_END"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "phase", "type": "string", "required": True},
                {"name": "passed", "type": "boolean", "required": True},
                {
                    "name": "violations",
                    "type": "GuardrailViolation[]",
                    "required": True,
                },
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )

    def test_phase_has_enum(self, fields: list[dict[str, Any]]) -> None:
        field = next(f for f in fields if f["name"] == "phase")
        assert "enum" in field
        assert "pre" in field["enum"]
        assert "post" in field["enum"]


class TestGuardrailCallbackStartFieldSchema:
    """Tests for GUARDRAIL_CALLBACK_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "GUARDRAIL_CALLBACK_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "callbackId", "type": "string", "required": True},
                {"name": "index", "type": "number", "required": True},
                {"name": "ruleId", "type": "string", "required": True},
            ],
        )


class TestGuardrailCallbackEndFieldSchema:
    """Tests for GUARDRAIL_CALLBACK_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "GUARDRAIL_CALLBACK_END"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "callbackId", "type": "string", "required": True},
                {"name": "index", "type": "number", "required": True},
                {"name": "ruleId", "type": "string", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
                {"name": "success", "type": "boolean", "required": True},
                {"name": "error", "type": "string", "required": False},
            ],
        )


# Drift Events
class TestDriftCheckResultFieldSchema:
    """Tests for DRIFT_CHECK_RESULT event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "DRIFT_CHECK_RESULT"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "detected", "type": "boolean", "required": True},
                {"name": "score", "type": "number", "required": True},
                {
                    "name": "metrics",
                    "type": "Record<string, unknown>",
                    "required": True,
                },
                {"name": "threshold", "type": "number", "required": True},
            ],
        )


class TestDriftCheckSkippedFieldSchema:
    """Tests for DRIFT_CHECK_SKIPPED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "DRIFT_CHECK_SKIPPED"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "reason", "type": "string", "required": True},
            ],
        )


# Checkpoint/Resume Events
class TestCheckpointSavedFieldSchema:
    """Tests for CHECKPOINT_SAVED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["CHECKPOINT_SAVED"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "checkpoint", "type": "string", "required": True},
                {"name": "tokenCount", "type": "number", "required": True},
            ],
        )


class TestResumeStartFieldSchema:
    """Tests for RESUME_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["RESUME_START"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "checkpoint", "type": "string", "required": True},
                {"name": "tokenCount", "type": "number", "required": True},
            ],
        )


class TestContinuationStartFieldSchema:
    """Tests for CONTINUATION_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "CONTINUATION_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "checkpoint", "type": "string", "required": True},
                {"name": "tokenCount", "type": "number", "required": True},
            ],
        )


# Retry Events
class TestAttemptStartFieldSchema:
    """Tests for ATTEMPT_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["ATTEMPT_START"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "attempt", "type": "number", "required": True},
                {"name": "isRetry", "type": "boolean", "required": True},
                {"name": "isFallback", "type": "boolean", "required": True},
            ],
        )


class TestRetryStartFieldSchema:
    """Tests for RETRY_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["RETRY_START"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "maxAttempts", "type": "number", "required": True},
            ],
        )


class TestRetryAttemptFieldSchema:
    """Tests for RETRY_ATTEMPT event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["RETRY_ATTEMPT"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "attempt", "type": "number", "required": True},
                {"name": "maxAttempts", "type": "number", "required": True},
                {"name": "reason", "type": "string", "required": True},
                {"name": "delayMs", "type": "number", "required": True},
            ],
        )


class TestRetryEndFieldSchema:
    """Tests for RETRY_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["RETRY_END"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "attempt", "type": "number", "required": True},
                {"name": "success", "type": "boolean", "required": True},
            ],
        )


class TestRetryGiveUpFieldSchema:
    """Tests for RETRY_GIVE_UP event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["RETRY_GIVE_UP"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "attempt", "type": "number", "required": True},
                {"name": "maxAttempts", "type": "number", "required": True},
                {"name": "reason", "type": "string", "required": True},
            ],
        )


# Fallback Events
class TestFallbackStartFieldSchema:
    """Tests for FALLBACK_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["FALLBACK_START"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "fromIndex", "type": "number", "required": True},
                {"name": "toIndex", "type": "number", "required": True},
            ],
        )


class TestFallbackModelSelectedFieldSchema:
    """Tests for FALLBACK_MODEL_SELECTED event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "FALLBACK_MODEL_SELECTED"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "index", "type": "number", "required": True},
                {"name": "reason", "type": "string", "required": True},
            ],
        )


class TestFallbackEndFieldSchema:
    """Tests for FALLBACK_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["FALLBACK_END"][
            "fields"
        ]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "success", "type": "boolean", "required": True},
                {"name": "finalIndex", "type": "number", "required": True},
            ],
        )


# Completion Events
class TestErrorFieldSchema:
    """Tests for ERROR event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["ERROR"]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "error", "type": "string", "required": True},
                {"name": "errorCode", "type": "string", "required": False},
                {"name": "failureType", "type": "FailureType", "required": True},
                {
                    "name": "recoveryStrategy",
                    "type": "RecoveryStrategy",
                    "required": True,
                },
                {"name": "policy", "type": "RecoveryPolicy", "required": True},
            ],
        )


class TestCompleteFieldSchema:
    """Tests for COMPLETE event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"]["COMPLETE"]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "tokenCount", "type": "number", "required": True},
                {"name": "contentLength", "type": "number", "required": True},
                {"name": "state", "type": "L0State", "required": False},
            ],
        )


# Structured Output Events
class TestStructuredParseStartFieldSchema:
    """Tests for STRUCTURED_PARSE_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_PARSE_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "contentLength", "type": "number", "required": True},
            ],
        )


class TestStructuredParseEndFieldSchema:
    """Tests for STRUCTURED_PARSE_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_PARSE_END"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "success", "type": "boolean", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


class TestStructuredParseErrorFieldSchema:
    """Tests for STRUCTURED_PARSE_ERROR event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_PARSE_ERROR"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "error", "type": "string", "required": True},
                {"name": "contentPreview", "type": "string", "required": False},
            ],
        )


class TestStructuredValidationStartFieldSchema:
    """Tests for STRUCTURED_VALIDATION_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_VALIDATION_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "schemaName", "type": "string", "required": False},
            ],
        )


class TestStructuredValidationEndFieldSchema:
    """Tests for STRUCTURED_VALIDATION_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_VALIDATION_END"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "valid", "type": "boolean", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


class TestStructuredValidationErrorFieldSchema:
    """Tests for STRUCTURED_VALIDATION_ERROR event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_VALIDATION_ERROR"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "errors", "type": "string[]", "required": True},
            ],
        )


class TestStructuredAutoCorrectStartFieldSchema:
    """Tests for STRUCTURED_AUTO_CORRECT_START event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_AUTO_CORRECT_START"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "errorCount", "type": "number", "required": True},
            ],
        )


class TestStructuredAutoCorrectEndFieldSchema:
    """Tests for STRUCTURED_AUTO_CORRECT_END event field schema."""

    @pytest.fixture
    def fields(self) -> list[dict[str, Any]]:
        return SPEC["monitoring"]["observabilityEvents"]["events"][
            "STRUCTURED_AUTO_CORRECT_END"
        ]["fields"]

    def test_has_exact_fields(self, fields: list[dict[str, Any]]) -> None:
        validate_exact_fields(
            fields,
            [
                {"name": "success", "type": "boolean", "required": True},
                {"name": "correctionsMade", "type": "number", "required": True},
                {"name": "durationMs", "type": "number", "required": True},
            ],
        )


# ============================================================================
# Lifecycle Invariants Tests
# ============================================================================


class TestLifecycleInvariants:
    """Tests for documented lifecycle invariants."""

    @pytest.fixture
    def invariants(self) -> list[dict[str, str]]:
        return SPEC["lifecycleInvariants"]["invariants"]

    def test_documents_all_critical_invariants(
        self, invariants: list[dict[str, str]]
    ) -> None:
        invariant_ids = [i["id"] for i in invariants]
        expected = [
            "session-start-once",
            "attempt-start-retries-only",
            "fallback-not-attempt",
            "retry-precedes-attempt",
            "timestamps-monotonic",
            "stream-id-consistent",
            "context-immutable",
            "context-propagated",
        ]
        for inv_id in expected:
            assert inv_id in invariant_ids, f"Invariant {inv_id} should be documented"

    def test_invariants_have_rule_and_rationale(
        self, invariants: list[dict[str, str]]
    ) -> None:
        for inv in invariants:
            assert "rule" in inv and inv["rule"], (
                f"Invariant {inv['id']} should have a rule"
            )
            assert "rationale" in inv and inv["rationale"], (
                f"Invariant {inv['id']} should have a rationale"
            )


# Generate parametrized tests for each invariant
_INVARIANTS = SPEC["lifecycleInvariants"]["invariants"]


@pytest.mark.parametrize(
    "invariant",
    _INVARIANTS,
    ids=[i["id"] for i in _INVARIANTS],
)
def test_invariant_has_rule_and_rationale(invariant: dict[str, str]) -> None:
    """Each invariant should have rule and rationale."""
    assert invariant.get("rule"), f"Invariant {invariant['id']} should have a rule"
    assert invariant.get("rationale"), (
        f"Invariant {invariant['id']} should have a rationale"
    )
