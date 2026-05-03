"""Single pipeline stage."""

import asyncio
from collections.abc import Awaitable, Callable
from inspect import isawaitable

from async_pipeline.errors import StageExecutionError


class Stage[T, U]:
    """A named step that transforms input into output (sync or async)."""

    __slots__ = ("_handler", "name", "timeout")

    def __init__(
        self,
        name: str,
        handler: Callable[[T], U] | Callable[[T], Awaitable[U]],
        *,
        timeout: float | None = None,
    ) -> None:
        if timeout is not None and timeout <= 0:
            msg = "timeout must be greater than 0"
            raise ValueError(msg)
        self.name = name
        self._handler = handler
        self.timeout = timeout

    async def run(self, value: T) -> U:
        try:
            result = self._handler(value)
        except Exception as exc:
            raise StageExecutionError(self.name, exc) from exc

        if isawaitable(result):
            try:
                if self.timeout is not None:
                    async with asyncio.timeout(self.timeout):
                        return await result
                return await result
            except TimeoutError as exc:
                raise StageExecutionError(self.name, exc) from exc
            except Exception as exc:
                raise StageExecutionError(self.name, exc) from exc

        return result
