"""Pipeline of sequential stages with optional execution context and lifecycle hooks."""

import asyncio
from collections.abc import Iterable, Sequence
from typing import Any, Literal, cast, overload

from async_pipeline._hooks import (
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
            raise ValueError("Pipeline requires at least one stage")
        self._stages = tuple(stages)
        self._before_hook: BeforeHookRunner | None = (
            normalize_before_hook(before_stage) if before_stage is not None else None
        )
        self._after_hook: AfterHookRunner | None = (
            normalize_after_hook(after_stage) if after_stage is not None else None
        )

    async def run(
        self,
        initial_value: T,
        *,
        context: dict[str, Any] | None = None,
    ) -> U:
        """Run each stage in order. The same context dict is shared across stages."""
        ctx: dict[str, Any] = {} if context is None else context
        value: Any = initial_value
        for stage in self._stages:
            value = await self._run_stage(stage, value, ctx)
        return cast(U, value)

    async def _run_stage(
        self,
        stage: Stage[Any, Any],
        value: Any,
        ctx: dict[str, Any],
    ) -> Any:
        """Run one stage with before/after hooks; reraises stage failures."""
        await self._call_before(stage.name, value, ctx)
        try:
            output = await stage.run(value, context=ctx)
        except Exception as exc:
            await self._call_after(stage.name, value, None, exc, ctx)
            raise
        await self._call_after(stage.name, value, output, None, ctx)
        return output

    async def _call_before(self, name: str, value: Any, ctx: dict[str, Any]) -> None:
        if self._before_hook is None:
            return
        try:
            await self._before_hook(name, value, ctx)
        except Exception:
            return

    async def _call_after(
        self,
        name: str,
        value: Any,
        output: Any | None,
        error: Exception | None,
        ctx: dict[str, Any],
    ) -> None:
        if self._after_hook is None:
            return
        try:
            await self._after_hook(name, value, output, error, ctx)
        except Exception:
            return

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
        """Run pipeline per item with bounded concurrency; preserves input order."""
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        seq = list(items)
        if not seq:
            return []
        results: list[U | Exception | None] = [None] * len(seq)
        semaphore = asyncio.Semaphore(concurrency)
        template: dict[str, Any] = {} if context is None else context

        async def worker(index: int, item: T) -> None:
            async with semaphore:
                ctx = dict(template)
                try:
                    results[index] = await self.run(item, context=ctx)
                except Exception as exc:
                    if not return_exceptions:
                        raise
                    results[index] = exc

        async with asyncio.TaskGroup() as tg:
            for index, item in enumerate(seq):
                tg.create_task(worker(index, item))

        return cast(list[U] | list[U | Exception], results)
