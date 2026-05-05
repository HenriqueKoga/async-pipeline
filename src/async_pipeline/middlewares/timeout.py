"""Timeout middleware using ``asyncio.timeout``."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


class TimeoutMiddleware:
    """Bound ``await next(value)`` with :func:`asyncio.timeout`.

    On expiry, propagates :class:`TimeoutError`. Composes with
    :class:`RetryMiddleware` and with :class:`~async_pipeline.Stage` ``timeout``
    (inner stage timeouts still apply inside ``Stage.run``).

    Raises:
        ValueError: If ``timeout`` is not strictly positive.
    """

    def __init__(self, timeout: float) -> None:
        """Args:
            timeout: Maximum seconds for the inner chain hop.
        """
        if timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        self._timeout = timeout

    async def __call__(
        self,
        next_fn: Callable[[Any], Awaitable[Any]],
        _stage_name: str,
        value: Any,
        _context: object,
    ) -> Any:
        async with asyncio.timeout(self._timeout):
            return await next_fn(value)
