from __future__ import annotations

import random
import time
from functools import wraps
from typing import TYPE_CHECKING, Callable, Type

if TYPE_CHECKING:
    from watchdog.config import RetryConfig


class RetryExhausted(Exception):
    pass


def retry_with_backoff(
    config: RetryConfig,
    retryable: tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            deadline = time.monotonic() + (config.total_deadline_ms / 1000.0)
            last_exception = None

            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                except retryable as e:
                    last_exception = e
                    if attempt == config.max_attempts - 1:
                        raise RetryExhausted(
                            f"{func.__name__} failed after {config.max_attempts} attempts"
                        ) from e

                    if time.monotonic() > deadline:
                        raise RetryExhausted(
                            f"{func.__name__} deadline exceeded after {attempt + 1} attempts"
                        ) from e

                    delay = min(
                        config.base_delay_ms * (2**attempt),
                        config.max_delay_ms,
                    )
                    if config.jitter:
                        delay = delay * (0.5 + random.random())

                    time.sleep(delay / 1000.0)

            if last_exception:
                raise last_exception
            return None

        return wrapper

    return decorator
