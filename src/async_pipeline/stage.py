"""Single pipeline stage with optional timeout and retry policy."""

import asyncio
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from typing import Any, Literal

from async_pipeline._hooks import accepts_arity
from async_pipeline.errors import StageExecutionError

BackoffMode = Literal["fixed", "exponential"]
_VALID_BACKOFFS: tuple[BackoffMode, ...] = ("fixed", "exponential")


def _retry_delay(base: float, mode: BackoffMode, attempt: int) -> float:
    """Delay before retry attempt N (1-based; attempt=1 is the first retry)."""
    if mode == "exponential":
        return float(base * 2 ** (attempt - 1))
    return base


class Stage[T, U]:
    """A named transformation step. Wraps a sync or async handler.

    Failures are surfaced as :class:`StageExecutionError` once retries (if
    any) are exhausted. ``timeout`` only applies to awaitable handlers.
    """

    __slots__ = (
        "_handler",
        "_handler_uses_context",
        "backoff",
        "name",
        "retries",
        "retry_delay",
        "timeout",
    )

    def __init__(
        self,
        name: str,
        handler: Callable[..., U] | Callable[..., Awaitable[U]],
        *,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: BackoffMode | str = "fixed",
    ) -> None:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        if retries < 0:
            raise ValueError("retries must be greater than or equal to 0")
        if retry_delay < 0:
            raise ValueError("retry_delay must be greater than or equal to 0")
        if backoff not in _VALID_BACKOFFS:
            raise ValueError('backoff must be "fixed" or "exponential"')

        self.name = name
        self._handler = handler
        self._handler_uses_context = accepts_arity(handler, 2)
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.backoff = backoff

    async def run(
        self,
        value: T,
        *,
        context: dict[str, Any] | None = None,
    ) -> U:
        """Run handler with retry + timeout; wraps failures in StageExecutionError."""
        ctx: dict[str, Any] = {} if context is None else context
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            if attempt > 0:
                delay = _retry_delay(self.retry_delay, self.backoff, attempt)
                if delay > 0:
                    await asyncio.sleep(delay)
            try:
                return await self._invoke_handler(value, ctx)
            except Exception as exc:
                last_error = exc
        assert last_error is not None  # loop runs at least once
        raise StageExecutionError(self.name, last_error) from last_error

    async def _invoke_handler(self, value: T, context: dict[str, Any]) -> U:
        """Run the handler once. Honors ``timeout`` for awaitable results only."""
        result = (
            self._handler(value, context)
            if self._handler_uses_context
            else self._handler(value)
        )
        if not isawaitable(result):
            return result
        if self.timeout is None:
            return await result
        async with asyncio.timeout(self.timeout):
            return await result
