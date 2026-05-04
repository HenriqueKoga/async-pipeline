"""Pipeline of sequential stages with optional execution context and lifecycle hooks."""

from collections.abc import Iterable, Sequence
from typing import Any, Literal, cast, overload

from async_pipeline._hooks import (
    AfterHookRunner,
    BeforeHookRunner,
    normalize_after_hook,
    normalize_before_hook,
)
from async_pipeline._parallel_map import map_ordered
from async_pipeline._pipeline_stage import run_stage_with_lifecycle
from async_pipeline.stage import Stage
from async_pipeline.types import AfterStageHook, BeforeStageHook, Middleware


class Pipeline[T, U]:
    """Sequential composition of :class:`Stage` callables.

    Each stage receives the previous stage's return value. Optional
    ``before_stage`` / ``after_stage`` hooks observe each step; ``middlewares``
    wrap ``Stage.run`` (outer list item runs first). Use :meth:`run` for one
    value and :meth:`map` for many inputs with bounded concurrency.

    Raises:
        ValueError: If ``stages`` is empty.
    """

    __slots__ = ("_after_hook", "_before_hook", "_middlewares", "_stages")

    def __init__(
        self,
        stages: Sequence[Stage[Any, Any]],
        *,
        before_stage: BeforeStageHook | None = None,
        after_stage: AfterStageHook | None = None,
        middlewares: Sequence[Middleware] | None = None,
    ) -> None:
        """Configure stages and optional hooks/middlewares.

        Args:
            stages: At least one stage, executed in order.
            before_stage: Called before each ``Stage.run`` (hook errors ignored).
            after_stage: Called after each ``Stage.run`` (hook errors ignored).
            middlewares: Outermost middleware is the first list element.
        """
        if not stages:
            raise ValueError("Pipeline requires at least one stage")
        self._stages = tuple(stages)
        self._before_hook: BeforeHookRunner | None = (
            normalize_before_hook(before_stage) if before_stage is not None else None
        )
        self._after_hook: AfterHookRunner | None = (
            normalize_after_hook(after_stage) if after_stage is not None else None
        )
        self._middlewares: tuple[Middleware, ...] = (
            tuple(middlewares) if middlewares is not None else ()
        )

    async def run(
        self,
        initial_value: T,
        *,
        context: dict[str, Any] | None = None,
    ) -> U:
        """Execute all stages for a single input.

        Args:
            initial_value: Input to the first stage.
            context: Shared mutable mapping for the whole run; if ``None``, an
                empty dict is created and reused for every stage and hook.

        Returns:
            The last stage's output.

        Raises:
            StageExecutionError: When a stage handler fails after its own
                retries/timeout policy (see :class:`Stage`).
        """
        ctx: dict[str, Any] = {} if context is None else context
        value: Any = initial_value
        for stage in self._stages:
            value = await run_stage_with_lifecycle(
                stage=stage,
                value=value,
                context=ctx,
                before_hook=self._before_hook,
                after_hook=self._after_hook,
                middlewares=self._middlewares,
            )
        return cast(U, value)

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
        """Run the same pipeline for each input with a concurrency cap.

        Results are aligned with ``items`` order. Each item gets a shallow copy
        of ``context`` (when provided) so workers do not share the same dict.

        Args:
            items: Iterable of inputs for the first stage.
            concurrency: Maximum concurrent ``run`` calls (minimum ``1``).
            context: Optional template dict copied per item.
            return_exceptions: If ``True``, store exceptions in the result list
                instead of cancelling siblings via ``TaskGroup``.

        Raises:
            ValueError: If ``concurrency`` is below ``1``.
            ExceptionGroup: When ``return_exceptions`` is ``False`` and any item
                fails (often wrapping :class:`StageExecutionError`).
        """

        async def execute(item: T, item_ctx: dict[str, Any]) -> U:
            return await self.run(item, context=item_ctx)

        return await map_ordered(
            items,
            concurrency=concurrency,
            return_exceptions=return_exceptions,
            template_context=context,
            execute=execute,
        )
