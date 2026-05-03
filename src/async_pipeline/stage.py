"""Single pipeline stage."""

import asyncio
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from typing import Any, Literal, cast

from async_pipeline._invocation import handler_wants_context
from async_pipeline.errors import StageExecutionError


class Stage[T, U]:
    """A named step that transforms input into output (sync or async)."""

    __slots__ = ("_handler", "backoff", "name", "retries", "retry_delay", "timeout")

    def __init__(
        self,
        name: str,
        handler: Callable[..., U] | Callable[..., Awaitable[U]],
        *,
        timeout: float | None = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        backoff: str = "fixed",
    ) -> None:
        if timeout is not None and timeout <= 0:
            msg = "timeout must be greater than 0"
            raise ValueError(msg)
        if retries < 0:
            msg = "retries must be greater than or equal to 0"
            raise ValueError(msg)
        if retry_delay < 0:
            msg = "retry_delay must be greater than or equal to 0"
            raise ValueError(msg)
        if backoff not in ("fixed", "exponential"):
            msg = 'backoff must be "fixed" or "exponential"'
            raise ValueError(msg)
        self.name = name
        self._handler = handler
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.backoff = cast(Literal["fixed", "exponential"], backoff)

    async def _execute_attempt(
        self,
        value: T,
        context: dict[str, Any],
    ) -> U:
        """Run the handler once (sync or async, with optional timeout)."""
        if handler_wants_context(self._handler):
            result = self._handler(value, context)
        else:
            result = self._handler(value)

        if isawaitable(result):
            if self.timeout is not None:
                async with asyncio.timeout(self.timeout):
                    return await result
            return await result

        return result

    async def run(
        self,
        value: T,
        *,
        context: dict[str, Any] | None = None,
    ) -> U:
        ctx: dict[str, Any] = {} if context is None else context
        attempt = 0
        while True:
            try:
                return await self._execute_attempt(value, ctx)
            except Exception as exc:
                attempt += 1
                if attempt > self.retries:
                    raise StageExecutionError(self.name, exc) from exc
                delay = self.retry_delay
                if self.backoff == "exponential":
                    delay *= 2 ** (attempt - 1)
                if delay > 0:
                    await asyncio.sleep(delay)
