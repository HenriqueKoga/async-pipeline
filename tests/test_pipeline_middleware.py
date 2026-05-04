"""Tests for Pipeline middleware chain."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from async_pipeline import Middleware, Pipeline, Stage, StageExecutionError


async def test_middleware_changes_output() -> None:
    async def add_one(x: int) -> int:
        return x + 1

    async def middleware(
        next_fn: Callable[[Any], Awaitable[Any]],
        _stage_name: str,
        value: Any,
        _context: dict[str, Any],
    ) -> Any:
        result = await next_fn(value)
        return int(result) * 2

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[middleware],
    )
    assert await pipeline.run(1) == 4


async def test_middleware_execution_order_outer_first() -> None:
    order: list[str] = []

    async def add_one(x: int) -> int:
        order.append("stage")
        return x + 1

    async def m1(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        order.append("m1-in")
        r = await next_fn(v)
        order.append("m1-out")
        return r

    async def m2(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        order.append("m2-in")
        r = await next_fn(v)
        order.append("m2-out")
        return r

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[m1, m2],
    )
    await pipeline.run(0)
    assert order == ["m1-in", "m2-in", "stage", "m2-out", "m1-out"]


async def test_middleware_modifies_input_to_stage() -> None:
    async def echo(x: int) -> int:
        return x

    async def add_ten(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        return await next_fn(int(v) + 10)

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("echo", echo)],
        middlewares=[add_ten],
    )
    assert await pipeline.run(3) == 13


async def test_middleware_captures_and_reraises_error() -> None:
    async def boom(_: int) -> int:
        raise RuntimeError("x")

    async def capture(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        ctx: dict[str, Any],
    ) -> Any:
        try:
            return await next_fn(v)
        except Exception as exc:
            ctx["caught"] = str(exc)
            raise

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("boom", boom)],
        middlewares=[capture],
    )
    ctx: dict[str, Any] = {}
    with pytest.raises(StageExecutionError) as excinfo:
        await pipeline.run(1, context=ctx)
    caught = ctx.get("caught")
    assert isinstance(caught, str)
    assert "x" in caught
    assert excinfo.value.__cause__ is not None
    assert str(excinfo.value.__cause__) == "x"


async def test_middleware_with_hooks_order() -> None:
    events: list[str] = []

    async def add_one(x: int) -> int:
        events.append("stage")
        return x + 1

    async def mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        events.append("mw")
        return await next_fn(v)

    def before(_n: str, _v: object) -> None:
        events.append("before")

    def after(
        _n: str,
        _v: object,
        _o: object | None,
        _e: Exception | None,
    ) -> None:
        events.append("after")

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        before_stage=before,
        after_stage=after,
        middlewares=[mw],
    )
    await pipeline.run(0)
    assert events == ["before", "mw", "stage", "after"]


async def test_middleware_with_context() -> None:
    async def add_one(x: int, context: dict[str, Any]) -> int:
        context["seen"] = True
        return x + 1

    async def mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        ctx: dict[str, Any],
    ) -> Any:
        ctx["mw"] = 1
        return await next_fn(v)

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[mw],
    )
    ctx: dict[str, Any] = {}
    assert await pipeline.run(1, context=ctx) == 2
    assert ctx.get("mw") == 1
    assert ctx.get("seen") is True


async def test_middleware_with_map() -> None:
    async def double(x: int) -> int:
        return x * 2

    async def mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        return int(await next_fn(v)) + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("double", double)],
        middlewares=[mw],
    )
    assert await pipeline.map([1, 2, 3], concurrency=2) == [3, 5, 7]


async def test_middleware_map_concurrency() -> None:
    lock = asyncio.Lock()
    mw_calls = 0

    async def inc(x: int) -> int:
        return x + 1

    async def count_mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        nonlocal mw_calls
        async with lock:
            mw_calls += 1
        return await next_fn(v)

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("inc", inc)],
        middlewares=[count_mw],
    )
    await pipeline.map([1, 2, 3, 4], concurrency=2)
    assert mw_calls == 4


async def test_pipeline_without_middleware_unchanged() -> None:
    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add_one", add_one)])
    assert await pipeline.run(5) == 6


async def test_middleware_coexists_with_stage_retry() -> None:
    """Middleware wraps ``Stage.run``; retries stay inside the stage."""

    calls_to_next = 0
    state = {"fail_once": True}

    async def flaky(x: int) -> int:
        if state["fail_once"]:
            state["fail_once"] = False
            raise RuntimeError("once")
        return x + 1

    async def mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        nonlocal calls_to_next
        calls_to_next += 1
        return await next_fn(v)

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("flaky", flaky, retries=2, retry_delay=0.01)],
        middlewares=[mw],
    )
    assert await pipeline.run(0) == 1
    assert calls_to_next == 1


async def test_middleware_coexists_with_stage_timeout() -> None:
    async def fast(x: int) -> int:
        return x + 1

    async def mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        return await next_fn(v)

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("fast", fast, timeout=1.0)],
        middlewares=[mw],
    )
    assert await pipeline.run(3) == 4


def test_middleware_type_alias_assignable() -> None:
    async def mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        return await next_fn(v)

    _: Middleware = mw
