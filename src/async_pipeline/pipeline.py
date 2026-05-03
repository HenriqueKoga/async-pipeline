"""Pipeline of sequential stages."""

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import Any, Literal, cast, overload

from async_pipeline._invocation import (
    AfterHookRunner,
    BeforeHookRunner,
    normalize_after_hook,
    normalize_before_hook,
)
from async_pipeline.stage import Stage
from async_pipeline.types import AfterStageHook, BeforeStageHook


class Pipeline[T, U]:
    """Runs stages in order, passing each output as the next input."""

    __slots__ = ("_after_hook", "_before_hook", "_stages")

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
        self._before_hook: BeforeHookRunner | None = (
            normalize_before_hook(before_stage) if before_stage is not None else None
        )
        self._after_hook: AfterHookRunner | None = (
            normalize_after_hook(after_stage) if after_stage is not None else None
        )

    @staticmethod
    async def _try_hook(
        hook: Callable[..., Awaitable[None]] | None,
        *args: Any,
    ) -> None:
        if hook is None:
            return
        try:
            await hook(*args)
        except Exception:
            return

    async def run(
        self,
        initial_value: T,
        *,
        context: dict[str, Any] | None = None,
    ) -> U:
        ctx: dict[str, Any] = {} if context is None else context
        current_value: Any = initial_value
        for stage in self._stages:
            stage_input = current_value
            await self._try_hook(
                self._before_hook,
                stage.name,
                stage_input,
                ctx,
            )
            try:
                stage_output = await stage.run(stage_input, context=ctx)
            except Exception as exc:
                await self._try_hook(
                    self._after_hook,
                    stage.name,
                    stage_input,
                    None,
                    exc,
                    ctx,
                )
                raise
            await self._try_hook(
                self._after_hook,
                stage.name,
                stage_input,
                stage_output,
                None,
                ctx,
            )
            current_value = stage_output
        return cast(U, current_value)

    @overload
    async def map(
        self,
        items: Iterable[T],
        concurrency: int = 5,
        *,
        context: dict[str, Any] | None = None,
        return_exceptions: Literal[False] = False,
    ) -> list[U]: ...

    @overload
    async def map(
        self,
        items: Iterable[T],
        concurrency: int = 5,
        *,
        context: dict[str, Any] | None = None,
        return_exceptions: Literal[True],
    ) -> list[U | Exception]: ...

    async def map(
        self,
        items: Iterable[T],
        concurrency: int = 5,
        *,
        context: dict[str, Any] | None = None,
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
        template: dict[str, Any] = {} if context is None else context

        async def worker(index: int, item: T) -> None:
            async with semaphore:
                ctx = dict(template)
                try:
                    result = await self.run(item, context=ctx)
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
