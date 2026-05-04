"""Timeout middleware using ``asyncio.timeout``."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


class TimeoutMiddleware:
    """Apply ``asyncio.timeout`` around ``await next(value)``."""

    def __init__(self, timeout: float) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        self._timeout = timeout

    async def __call__(
        self,
        next_fn: Callable[[Any], Awaitable[Any]],
        _stage_name: str,
        value: Any,
        _context: dict[str, Any],
    ) -> Any:
        async with asyncio.timeout(self._timeout):
            return await next_fn(value)
