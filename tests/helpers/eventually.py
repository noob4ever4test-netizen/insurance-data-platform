from collections.abc import Callable
from math import isfinite
from threading import Event
from time import monotonic
from typing import TypeVar

_Result = TypeVar("_Result")
_wait = Event().wait


def eventually(
    check: Callable[[], _Result], *, timeout: float = 5.0, interval: float = 0.05
) -> _Result:
    """Retry assertions or false predicates; propagate other exceptions immediately."""
    if not isfinite(timeout) or timeout < 0 or not isfinite(interval) or interval <= 0:
        raise ValueError("timeout must be finite and nonnegative; interval finite and positive")
    deadline = monotonic() + timeout
    while True:
        try:
            result = check()
            if result is False:
                raise AssertionError("Condition returned False")
            return result
        except AssertionError as failure:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Check did not pass within {timeout}s: {failure}") from failure
            _wait(min(interval, remaining))
