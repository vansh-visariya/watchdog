from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from watchdog.config import RetryConfig
from watchdog.retry import RetryExhausted, retry_with_backoff


class TestRetryBackoff:
    def test_succeeds_on_first_attempt(self) -> None:
        config = RetryConfig(max_attempts=3, base_delay_ms=10, max_delay_ms=100)

        @retry_with_backoff(config)
        def work():
            return "ok"

        assert work() == "ok"

    def test_retries_and_succeeds(self) -> None:
        config = RetryConfig(
            max_attempts=3,
            base_delay_ms=1,
            max_delay_ms=10,
            jitter=False,
            total_deadline_ms=5000,
        )
        call_count = [0]

        @retry_with_backoff(config)
        def work():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("transient")
            return "recovered"

        result = work()
        assert result == "recovered"
        assert call_count[0] == 3

    def test_exhausts_retries(self) -> None:
        config = RetryConfig(max_attempts=2, base_delay_ms=1, max_delay_ms=5, jitter=False)

        @retry_with_backoff(config)
        def work():
            raise ValueError("persistent")

        with pytest.raises(RetryExhausted):
            work()

    def test_deadline_exceeded(self) -> None:
        config = RetryConfig(
            max_attempts=10,
            base_delay_ms=100,
            max_delay_ms=200,
            jitter=False,
            total_deadline_ms=50,
        )

        @retry_with_backoff(config)
        def work():
            time.sleep(0.05)
            raise ValueError("slow")

        with pytest.raises(RetryExhausted):
            work()

    def test_non_retryable_exception_passes_through(self) -> None:
        config = RetryConfig(max_attempts=3)

        @retry_with_backoff(config, retryable=(ValueError,))
        def work():
            raise TypeError("not retryable")

        with pytest.raises(TypeError):
            work()

    def test_jitter_adds_variation(self) -> None:
        config = RetryConfig(
            max_attempts=5,
            base_delay_ms=100,
            max_delay_ms=1000,
            jitter=True,
            total_deadline_ms=5000,
        )
        call_count = [0]

        @retry_with_backoff(config)
        def work():
            call_count[0] += 1
            if call_count[0] < 4:
                raise ValueError("transient")
            return "ok"

        result = work()
        assert result == "ok"
