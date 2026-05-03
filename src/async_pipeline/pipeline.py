"""Pipeline of sequential stages."""

import asyncio
from collections.abc import Callable, Iterable, Sequence
from inspect import isawaitable
from typing import Any, Literal, cast, overload

from async_pipeline.stage import Stage
from async_pipeline.types import AfterStageHook, BeforeStageHook


class Pipeline[T, U]:
    """Runs stages in order, passing each output as the next input."""

    __slots__ = ("_after_stage", "_before_stage", "_stages")

    def __init__(
        self,
        stages: Sequence[Stage[Any, Any]],
        *,
        before_stage: BeforeStageHook | None = None,
        after_stage: AfterStageHook | None = None,
    ) -> None:
        if not stages:
            msg = "Pipeline requires at least one stage"
            raise ValueError(msg)
        self._stages = tuple(stages)
        self._before_stage = before_stage
        self._after_stage = after_stage

    async def _call_hook(self, hook: Callable[..., Any], *args: Any) -> None:
        try:
            result = hook(*args)
            if isawaitable(result):
                await result
        except Exception:
            return

    async def run(self, initial_value: T) -> U:
        current_value: Any = initial_value
        for stage in self._stages:
            stage_input = current_value
            if self._before_stage is not None:
                await self._call_hook(self._before_stage, stage.name, stage_input)
            try:
                stage_output = await stage.run(stage_input)
            except Exception as exc:
                if self._after_stage is not None:
                    await self._call_hook(
                        self._after_stage,
                        stage.name,
                        stage_input,
                        None,
                        exc,
                    )
                raise
            if self._after_stage is not None:
                await self._call_hook(
                    self._after_stage,
                    stage.name,
                    stage_input,
                    stage_output,
                    None,
                )
            current_value = stage_output
        return cast(U, current_value)

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
