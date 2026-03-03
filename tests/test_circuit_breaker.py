"""
Unit tests for CircuitBreaker utility.
"""

import pytest
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError, CircuitState


class TestCircuitBreakerStates:
    """Test circuit breaker state transitions."""

    def test_initial_state_is_closed(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        assert cb.state == CircuitState.CLOSED

    def test_stays_closed_on_success(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_success()
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()  # Reset
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED  # Still closed, only 2 consecutive

    def test_open_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_closes_after_success_threshold(self):
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=0.1, success_threshold=2)
        cb.record_failure()
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN  # Need 2 successes
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_half_open_reopens_on_failure(self):
        cb = CircuitBreaker(name="test", failure_threshold=1, recovery_timeout=0.1)
        cb.record_failure()
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_manual_reset(self):
        cb = CircuitBreaker(name="test", failure_threshold=1)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED


class TestCircuitBreakerCall:
    """Test the call() method."""

    def test_call_passes_through_when_closed(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        result = cb.call(lambda: 42)
        assert result == 42

    def test_call_raises_when_open(self):
        cb = CircuitBreaker(name="test", failure_threshold=1)
        cb.record_failure()
        with pytest.raises(CircuitBreakerError, match="OPEN"):
            cb.call(lambda: 42)

    def test_call_records_success_on_success(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.call(lambda: "ok")
        assert cb._failure_count == 0  # Reset by success

    def test_call_records_failure_on_exception(self):
        cb = CircuitBreaker(name="test", failure_threshold=3)

        def failing():
            raise ValueError("boom")

        with pytest.raises(ValueError):
            cb.call(failing)
        assert cb._failure_count == 1

    def test_call_opens_circuit_after_repeated_failures(self):
        cb = CircuitBreaker(name="test", failure_threshold=2)

        def failing():
            raise RuntimeError("down")

        with pytest.raises(RuntimeError):
            cb.call(failing)
        with pytest.raises(RuntimeError):
            cb.call(failing)
        # Now circuit is open
        with pytest.raises(CircuitBreakerError):
            cb.call(failing)

    def test_call_passes_args(self):
        cb = CircuitBreaker(name="test")

        def add(a, b, offset=0):
            return a + b + offset

        assert cb.call(add, 2, 3, offset=10) == 15
