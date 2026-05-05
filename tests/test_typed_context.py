"""Tests for typed/custom context support."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, TypedDict
from unittest.mock import MagicMock

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError
from async_pipeline.middlewares import TimingMiddleware
from async_pipeline.telemetry.opentelemetry import OpenTelemetryMiddleware


class MyTypedContext(TypedDict):
    request_id: str
    count: int


@dataclass
class MyContext:
    request_id: str
    count: int = 0
    timings: dict[str, list[float]] | None = None
    trace_attributes: dict[str, str] | None = None


async def test_run_dict_context_backward_compatible() -> None:
    async def handler(value: int, context: dict[str, Any]) -> int:
        context["count"] = int(context.get("count", 0)) + 1
        return value + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("h", handler)])
    ctx: dict[str, Any] = {"count": 0}
    assert await pipeline.run(1, context=ctx) == 2
    assert ctx["count"] == 1


async def test_run_typed_dict_context() -> None:
    async def handler(value: int, context: MyTypedContext) -> int:
        context["count"] += 1
        return value + 1

    pipeline: Pipeline[int, int, MyTypedContext] = Pipeline([Stage("h", handler)])
    context: MyTypedContext = {"request_id": "abc", "count": 0}
    assert await pipeline.run(1, context=context) == 2
    assert context["count"] == 1


async def test_run_dataclass_context() -> None:
    async def handler(value: int, context: MyContext) -> int:
        context.count += 1
        return value + 1

    context = MyContext(request_id="abc")
    pipeline: Pipeline[int, int, MyContext] = Pipeline([Stage("handler", handler)])
    result = await pipeline.run(1, context=context)
    assert result == 2
    assert context.count == 1


async def test_handler_without_context_still_works() -> None:
    async def handler(value: int) -> int:
        return value + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("h", handler)])
    assert await pipeline.run(1, context=MyContext("abc")) == 2


async def test_handler_and_hooks_receive_same_context_object() -> None:
    seen_ids: list[int] = []

    async def handler(value: int, context: MyContext) -> int:
        seen_ids.append(id(context))
        return value + 1

    def before(_name: str, _value: object, context: MyContext) -> None:
        seen_ids.append(id(context))

    def after(
        _name: str,
        _value: object,
        _output: object | None,
        _error: Exception | None,
        context: MyContext,
    ) -> None:
        seen_ids.append(id(context))

    ctx = MyContext("abc")
    pipeline: Pipeline[int, int, MyContext] = Pipeline(
        [Stage("h", handler)],
        before_stage=before,
        after_stage=after,
    )
    await pipeline.run(1, context=ctx)
    assert seen_ids == [id(ctx), id(ctx), id(ctx)]


async def test_legacy_hooks_still_work() -> None:
    calls: list[tuple[str, object]] = []

    async def handler(value: int) -> int:
        return value + 1

    def before(stage_name: str, value: object) -> None:
        calls.append((stage_name, value))

    pipeline: Pipeline[int, int] = Pipeline([Stage("h", handler)], before_stage=before)
    await pipeline.run(1, context=MyContext("abc"))
    assert calls == [("h", 1)]


async def test_middleware_receives_custom_context_object() -> None:
    seen_ids: list[int] = []

    async def handler(value: int, context: MyContext) -> int:
        seen_ids.append(id(context))
        return value + 1

    async def middleware(
        next_fn: Any,
        _stage: str,
        value: Any,
        context: object,
    ) -> Any:
        seen_ids.append(id(context))
        return await next_fn(value)

    ctx = MyContext("abc")
    pipeline: Pipeline[int, int, MyContext] = Pipeline(
        [Stage("h", handler)],
        middlewares=[middleware],
    )
    await pipeline.run(1, context=ctx)
    assert seen_ids == [id(ctx), id(ctx)]


async def test_map_copies_dict_context_per_item() -> None:
    async def handler(value: int, context: dict[str, Any]) -> int:
        context["count"] = int(context.get("count", 0)) + value
        return int(context["count"])

    template: dict[str, Any] = {"count": 0}
    pipeline: Pipeline[int, int, dict[str, Any]] = Pipeline([Stage("h", handler)])
    assert await pipeline.map([1, 2, 3], context=template, concurrency=3) == [1, 2, 3]
    assert template["count"] == 0


async def test_map_copies_dataclass_context_per_item() -> None:
    @dataclass
    class Ctx:
        count: int = 0

    async def handler(value: int, context: Ctx) -> int:
        context.count += value
        return context.count

    pipeline: Pipeline[int, int, Ctx] = Pipeline([Stage("handler", handler)])
    assert await pipeline.map([1, 2, 3], context=Ctx(), concurrency=3) == [1, 2, 3]


async def test_timing_middleware_with_dataclass_context() -> None:
    async def add_one(value: int, context: MyContext) -> int:
        context.count += 1
        return value + 1

    ctx = MyContext("abc")
    pipeline: Pipeline[int, int, MyContext] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[TimingMiddleware()],
    )
    assert await pipeline.run(1, context=ctx) == 2
    assert ctx.timings is not None
    assert "add_one" in ctx.timings
    assert ctx.timings["add_one"][0] >= 0


async def test_context_none_still_works() -> None:
    async def add_one(value: int, context: dict[str, Any]) -> int:
        context["seen"] = True
        return value + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add_one", add_one)])
    assert await pipeline.run(1, context=None) == 2


async def test_retry_timeout_with_dataclass_context() -> None:
    calls = 0

    async def flaky(value: int, context: MyContext) -> int:
        nonlocal calls
        calls += 1
        context.count += 1
        if calls == 1:
            await asyncio.sleep(0.02)
            raise RuntimeError("retry")
        return value + 1

    stage = Stage("flaky", flaky, timeout=1.0, retries=1, retry_delay=0.0)
    ctx = MyContext("abc")
    assert await stage.run(1, context=ctx) == 2
    assert ctx.count == 2


async def test_opentelemetry_reads_trace_attributes_from_object() -> None:
    async def ident(value: int) -> int:
        return value

    span = MagicMock()
    span_ctx = MagicMock()
    span_ctx.__enter__ = MagicMock(return_value=span)
    span_ctx.__exit__ = MagicMock(return_value=False)
    tracer = MagicMock()
    tracer.start_as_current_span.return_value = span_ctx
    middleware = OpenTelemetryMiddleware()
    middleware._tracer = tracer  # type: ignore[attr-defined]

    context = MyContext(request_id="abc", trace_attributes={"tenant": "t1"})
    pipeline: Pipeline[int, int, MyContext] = Pipeline(
        [Stage("ident", ident)],
        middlewares=[middleware],
    )
    assert await pipeline.run(1, context=context) == 1
    span.set_attribute.assert_any_call("tenant", "t1")


async def test_timing_middleware_does_not_break_non_assignable_context() -> None:
    class Frozen:
        __slots__ = ()

    async def handler(value: int) -> int:
        return value + 1

    pipeline: Pipeline[int, int, Frozen] = Pipeline(
        [Stage("h", handler)],
        middlewares=[TimingMiddleware()],
    )
    assert await pipeline.run(1, context=Frozen()) == 2


async def test_stage_error_with_custom_context_type() -> None:
    async def boom(_: int, context: MyContext) -> int:
        context.count += 1
        raise RuntimeError("x")

    pipeline: Pipeline[int, int, MyContext] = Pipeline([Stage("boom", boom)])
    ctx = MyContext("abc")
    with pytest.raises(StageExecutionError):
        await pipeline.run(0, context=ctx)
    assert ctx.count == 1
