"""Run one pipeline stage: hooks, middleware chain, and ``Stage.run``."""

from collections.abc import Awaitable, Callable, Sequence
from inspect import isawaitable
from typing import Any

from async_pipeline._hooks import AfterHookRunner, BeforeHookRunner
from async_pipeline.stage import Stage
from async_pipeline.types import Middleware


def build_middleware_wrapped_runner(
    stage: Stage[Any, Any],
    context: dict[str, Any],
    middlewares: Sequence[Middleware],
) -> Callable[[Any], Awaitable[Any]]:
    """Return ``value -> ...`` with middlewares outer-first, ending in ``stage.run``."""

    async def run_stage(value: Any) -> Any:
        return await stage.run(value, context=context)

    handler: Callable[[Any], Awaitable[Any]] = run_stage
    for mw in reversed(middlewares):
        handler = _wrap_middleware_layer(handler, mw, stage.name, context)
    return handler


def _wrap_middleware_layer(
    next_handler: Callable[[Any], Awaitable[Any]],
    middleware: Middleware,
    stage_name: str,
    context: dict[str, Any],
) -> Callable[[Any], Awaitable[Any]]:
    async def wrapped(value: Any) -> Any:
        out = middleware(next_handler, stage_name, value, context)
        if isawaitable(out):
            return await out
        return out

    return wrapped


async def _invoke_hook_silently(
    hook: Callable[..., Awaitable[None]],
    *args: Any,
) -> None:
    try:
        await hook(*args)
    except Exception:
        return


async def run_stage_with_lifecycle(
    *,
    stage: Stage[Any, Any],
    value: Any,
    context: dict[str, Any],
    before_hook: BeforeHookRunner | None,
    after_hook: AfterHookRunner | None,
    middlewares: Sequence[Middleware],
) -> Any:
    """before → middleware chain → stage → after (success or failure)."""
    if before_hook is not None:
        await _invoke_hook_silently(before_hook, stage.name, value, context)

    runner = build_middleware_wrapped_runner(stage, context, middlewares)
    try:
        output = await runner(value)
    except Exception as exc:
        if after_hook is not None:
            await _invoke_hook_silently(
                after_hook, stage.name, value, None, exc, context
            )
        raise

    if after_hook is not None:
        await _invoke_hook_silently(
            after_hook, stage.name, value, output, None, context
        )
    return output
