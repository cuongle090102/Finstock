"""Circuit Breaker pattern for resilient service connections.

Prevents cascade failures by stopping calls to a failing service
after a threshold of consecutive failures, then periodically
retrying to check if the service has recovered.

States:
    CLOSED  - Normal operation, requests pass through
    OPEN    - Service assumed down, requests fail fast
    HALF_OPEN - Testing if service recovered (one request allowed)
"""

import time
import logging
import threading
from enum import Enum
from typing import Callable, Any, Optional
from functools import wraps

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open and blocking calls."""
    pass


class CircuitBreaker:
    """Thread-safe circuit breaker for external service calls.

    Args:
        name: Identifier for this breaker (used in logging)
        failure_threshold: Number of consecutive failures before opening circuit
        recovery_timeout: Seconds to wait before trying again (half-open)
        success_threshold: Consecutive successes in half-open to close circuit
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    logger.info(f"Circuit breaker '{self.name}' entering HALF_OPEN state")
            return self._state

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    logger.info(f"Circuit breaker '{self.name}' CLOSED (service recovered)")
            elif self._state != CircuitState.CLOSED:
                self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                logger.warning(f"Circuit breaker '{self.name}' back to OPEN (recovery failed)")
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                logger.error(
                    f"Circuit breaker '{self.name}' OPEN after {self._failure_count} failures"
                )

    def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute function through circuit breaker.

        Raises:
            CircuitBreakerError: If circuit is open
        """
        current_state = self.state
        if current_state == CircuitState.OPEN:
            raise CircuitBreakerError(
                f"Circuit breaker '{self.name}' is OPEN — service unavailable"
            )

        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except CircuitBreakerError:
            raise
        except Exception as e:
            self.record_failure()
            raise

    def reset(self) -> None:
        """Manually reset circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            logger.info(f"Circuit breaker '{self.name}' manually reset to CLOSED")


def circuit_breaker(breaker: CircuitBreaker):
    """Decorator to wrap a function with circuit breaker protection."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return breaker.call(func, *args, **kwargs)
        wrapper.circuit_breaker = breaker
        return wrapper
    return decorator
