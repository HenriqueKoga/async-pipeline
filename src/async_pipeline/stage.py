"""Single pipeline stage with optional timeout and retry policy."""

import asyncio
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from typing import Literal

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
    """A named transformation step wrapping a sync or async callable.

    Handlers may be ``(value)`` or ``(value, context)`` when the signature has
    at least two positional parameters. Failures become
    :class:`~async_pipeline.StageExecutionError` after all retries are used.
    ``timeout`` applies only when the handler returns an awaitable.
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
        """Configure the stage.

        Args:
            name: Stable identifier used in errors, hooks, and middleware.
            handler: Callable invoked by :meth:`run`.
            timeout: Seconds for ``asyncio.timeout`` around awaitable handlers;
                must be ``> 0`` when set.
            retries: Extra attempts after the first failure (total ``1 + retries``).
            retry_delay: Base seconds between retries; ``0`` skips ``sleep``.
            backoff: ``\"fixed\"`` or ``\"exponential\"`` delay growth between tries.

        Raises:
            ValueError: For invalid ``timeout``, ``retries``, ``retry_delay``, or
                ``backoff``.
        """
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
        context: object | None = None,
    ) -> U:
        """Invoke the handler with retry/timeout policy.

        Args:
            value: Input to the handler.
            context: Execution context; if ``None``, uses an empty dict.

        Returns:
            Handler result (possibly awaited).

        Raises:
            StageExecutionError: When every attempt raises ``Exception``.
        """
        ctx: object = {} if context is None else context
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

    async def _invoke_handler(self, value: T, context: object) -> U:
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
