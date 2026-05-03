"""Single pipeline stage."""

from collections.abc import Awaitable, Callable
from inspect import isawaitable

from async_pipeline.errors import StageExecutionError


class Stage[T, U]:
    """A named step that transforms input into output (sync or async)."""

    __slots__ = ("_handler", "name")

    def __init__(
        self,
        name: str,
        handler: Callable[[T], U] | Callable[[T], Awaitable[U]],
    ) -> None:
        self.name = name
        self._handler = handler

    async def run(self, value: T) -> U:
        try:
            result = self._handler(value)
        except Exception as exc:
            raise StageExecutionError(self.name, exc) from exc

        if isawaitable(result):
            try:
                return await result
            except Exception as exc:
                raise StageExecutionError(self.name, exc) from exc

        return result
