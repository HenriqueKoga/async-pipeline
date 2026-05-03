"""Pipeline of sequential stages."""

import asyncio
from collections.abc import Iterable, Sequence
from typing import Any, Literal, cast, overload

from async_pipeline.stage import Stage


class Pipeline[T, U]:
    """Runs stages in order, passing each output as the next input."""

    __slots__ = ("_stages",)

    def __init__(self, stages: Sequence[Stage[Any, Any]]) -> None:
        if not stages:
            msg = "Pipeline requires at least one stage"
            raise ValueError(msg)
        self._stages = tuple(stages)

    async def run(self, initial_value: T) -> U:
        value: Any = initial_value
        for stage in self._stages:
            value = await stage.run(value)
        return cast(U, value)

    @overload
    async def map(
        self,
        items: Iterable[T],
        concurrency: int = 5,
        *,
        return_exceptions: Literal[False] = False,
    ) -> list[U]: ...

    @overload
    async def map(
        self,
        items: Iterable[T],
        concurrency: int = 5,
        *,
        return_exceptions: Literal[True],
    ) -> list[U | Exception]: ...

    async def map(
        self,
        items: Iterable[T],
        concurrency: int = 5,
        *,
        return_exceptions: bool = False,
    ) -> list[U] | list[U | Exception]:
        """Run the pipeline per item with bounded concurrency; order matches inputs."""
        if concurrency < 1:
            msg = "concurrency must be at least 1"
            raise ValueError(msg)
        seq = list(items)
        n = len(seq)
        if n == 0:
            return []
        results: list[U | Exception | None] = [None] * n
        semaphore = asyncio.Semaphore(concurrency)

        async def worker(index: int, item: T) -> None:
            async with semaphore:
                try:
                    result = await self.run(item)
                    results[index] = result
                except Exception as exc:
                    if return_exceptions:
                        results[index] = exc
                    else:
                        raise

        async with asyncio.TaskGroup() as tg:
            for index, item in enumerate(seq):
                tg.create_task(worker(index, item))

        if return_exceptions:
            return cast(list[U | Exception], results)
        return cast(list[U], results)
