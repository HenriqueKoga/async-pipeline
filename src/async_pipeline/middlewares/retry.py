"""Retry middleware around ``next`` (in addition to any ``Stage`` retries)."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Literal

_Backoff = Literal["fixed", "exponential"]
_VALID: tuple[str, ...] = ("fixed", "exponential")


def _retry_delay(base: float, mode: _Backoff, attempt: int) -> float:
    """Delay before retry attempt ``attempt`` (1-based: first retry uses attempt=1)."""
    if mode == "exponential":
        return float(base * 2 ** (attempt - 1))
    return base


class RetryMiddleware:
    """Retry ``await next(value)`` on ``Exception`` (middleware-level).

    Total attempts are ``1 + retries``. Does not retry
    :class:`KeyboardInterrupt` or :class:`asyncio.CancelledError`. Independent
    of :class:`~async_pipeline.Stage` ``retries`` (both may apply).

    Raises:
        ValueError: For invalid configuration.
    """

    def __init__(
        self,
        retries: int = 3,
        delay: float = 0.0,
        backoff: _Backoff = "fixed",
    ) -> None:
        """Args:
            retries: Extra attempts after the first failure.
            delay: Base seconds between attempts; ``0`` skips ``sleep``.
            backoff: ``\"fixed\"`` or ``\"exponential\"`` backoff curve.
        """
        if retries < 0:
            raise ValueError("retries must be greater than or equal to 0")
        if delay < 0:
            raise ValueError("delay must be greater than or equal to 0")
        if backoff not in _VALID:
            raise ValueError('backoff must be "fixed" or "exponential"')
        self._retries = retries
        self._delay = delay
        self._backoff: _Backoff = backoff

    async def __call__(
        self,
        next_fn: Callable[[Any], Awaitable[Any]],
        _stage_name: str,
        value: Any,
        _context: dict[str, Any],
    ) -> Any:
        for attempt in range(self._retries + 1):
            if attempt > 0:
                sleep_for = _retry_delay(self._delay, self._backoff, attempt)
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
            try:
                return await next_fn(value)
            except (KeyboardInterrupt, asyncio.CancelledError):
                raise
            except Exception as exc:
                if attempt >= self._retries:
                    raise exc
