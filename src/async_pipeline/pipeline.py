"""Pipeline of sequential stages."""

import asyncio
from collections.abc import Callable, Iterable, Sequence
from inspect import isawaitable
from typing import Any, Literal, cast, overload

from async_pipeline._invocation import accepts_arity
from async_pipeline.stage import Stage
from async_pipeline.types import AfterStageHook, BeforeStageHook


class Pipeline[T, U]:
    """Runs stages in order, passing each output as the next input."""

    __slots__ = (
        "_after_stage",
        "_after_wants_context",
        "_before_stage",
        "_before_wants_context",
        "_stages",
    )

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
        self._before_wants_context = (
            before_stage is not None and accepts_arity(before_stage, 3)
        )
        self._after_wants_context = (
            after_stage is not None and accepts_arity(after_stage, 5)
        )

    @staticmethod
    async def _call_hook(
        hook: Callable[..., Any],
        args: tuple[Any, ...],
    ) -> None:
        try:
            result = hook(*args)
            if isawaitable(result):
                await result
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
            if self._before_stage is not None:
                args: tuple[Any, ...] = (
                    (stage.name, stage_input, ctx)
                    if self._before_wants_context
                    else (stage.name, stage_input)
                )
                await self._call_hook(self._before_stage, args)
            try:
                stage_output = await stage.run(stage_input, context=ctx)
            except Exception as exc:
                if self._after_stage is not None:
                    after_args: tuple[Any, ...] = (
                        (stage.name, stage_input, None, exc, ctx)
                        if self._after_wants_context
                        else (stage.name, stage_input, None, exc)
                    )
                    await self._call_hook(self._after_stage, after_args)
                raise
            if self._after_stage is not None:
                after_args = (
                    (stage.name, stage_input, stage_output, None, ctx)
                    if self._after_wants_context
                    else (stage.name, stage_input, stage_output, None)
                )
                await self._call_hook(self._after_stage, after_args)
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
