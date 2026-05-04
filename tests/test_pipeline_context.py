"""Tests for Pipeline execution context."""

import asyncio

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError


async def test_stage_receives_context() -> None:
    async def add_one(value: int, context: dict[str, object]) -> int:
        context["count"] = context.get("count", 0) + 1  # type: ignore[arg-type]
        return value + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add_one", add_one)])
    ctx: dict[str, object] = {"count": 0}
    result = await pipeline.run(1, context=ctx)
    assert result == 2
    assert ctx["count"] == 1


async def test_stage_one_arg_unchanged() -> None:
    async def add_one(value: int) -> int:
        return value + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add_one", add_one)])
    assert await pipeline.run(5, context={"ignored": True}) == 6


async def test_context_shared_across_stages() -> None:
    async def first(value: int, context: dict[str, object]) -> int:
        context["acc"] = value
        return value + 1

    async def second(value: int, context: dict[str, object]) -> int:
        return value + int(context["acc"])  # type: ignore[arg-type]

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("first", first), Stage("second", second)]
    )
    ctx: dict[str, object] = {}
    assert await pipeline.run(2, context=ctx) == 5
    assert ctx["acc"] == 2


async def test_hooks_receive_context() -> None:
    seen: list[tuple[str, dict[str, object]]] = []

    async def add_one(x: int) -> int:
        return x + 1

    def before(name: str, inp: object, context: dict[str, object]) -> None:
        seen.append(("before", dict(context)))
        context["seen_before"] = True

    def after(
        name: str,
        inp: object,
        out: object | None,
        err: Exception | None,
        context: dict[str, object],
    ) -> None:
        seen.append(("after", dict(context)))
        assert context.get("seen_before") is True
        assert err is None

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        before_stage=before,
        after_stage=after,
    )
    await pipeline.run(0, context={"k": 1})
    assert len(seen) == 2


async def test_legacy_hooks_without_context() -> None:
    before_calls: list[tuple[str, object]] = []

    async def add_one(x: int) -> int:
        return x + 1

    def before(name: str, inp: object) -> None:
        before_calls.append((name, inp))

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        before_stage=before,
    )
    await pipeline.run(4, context={"x": 1})
    assert before_calls == [("add_one", 4)]


async def test_run_context_none_creates_mutable_state() -> None:
    async def bump(_: int, context: dict[str, object]) -> int:
        context["n"] = int(context.get("n", 0)) + 1  # type: ignore[arg-type]
        return 0

    async def read_n(_: int, context: dict[str, object]) -> int:
        return int(context["n"])  # type: ignore[arg-type]

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("bump", bump), Stage("read_n", read_n)]
    )
    assert await pipeline.run(0, context=None) == 1


async def test_map_context_isolated() -> None:
    async def handler(x: int, context: dict[str, object]) -> int:
        context["x"] = x
        return x

    pipeline: Pipeline[int, int] = Pipeline([Stage("test", handler)])
    results = await pipeline.map([1, 2], context={"x": 0}, concurrency=2)
    assert results == [1, 2]


async def test_hook_context_on_stage_error() -> None:
    after_calls: list[Exception | None] = []

    async def boom(_: int) -> int:
        raise RuntimeError("no")

    def after(
        _n: str,
        _i: object,
        _o: object | None,
        err: Exception | None,
        ctx: dict[str, object],
    ) -> None:
        after_calls.append(err)
        ctx["after_ran"] = True

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("boom", boom)],
        after_stage=after,
    )
    ctx: dict[str, object] = {}
    with pytest.raises(StageExecutionError):
        await pipeline.run(0, context=ctx)
    assert len(after_calls) == 1
    assert isinstance(after_calls[0], StageExecutionError)
    assert ctx.get("after_ran") is True


async def test_stage_timeout_with_context() -> None:
    calls = 0

    async def slow(v: int, context: dict[str, object]) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(0.15)
        context["done"] = True
        return v

    stage = Stage("s", slow, timeout=0.08, retries=1, retry_delay=0.02)
    ctx: dict[str, object] = {}
    assert await stage.run(3, context=ctx) == 3
    assert ctx.get("done") is True


async def test_stage_retry_with_context() -> None:
    c = 0

    async def flaky(v: int, context: dict[str, object]) -> int:
        nonlocal c
        c += 1
        context["tries"] = c
        if c < 2:
            raise ValueError("retry")
        return v

    stage = Stage("f", flaky, retries=2)
    ctx: dict[str, object] = {}
    assert await stage.run(10, context=ctx) == 10
    assert ctx["tries"] == 2


async def test_map_return_exceptions_with_context() -> None:
    async def maybe(x: int, context: dict[str, object]) -> int:
        if x == 2:
            raise RuntimeError("bad")
        context["ok"] = True
        return x

    pipeline: Pipeline[int, int] = Pipeline([Stage("m", maybe)])
    out = await pipeline.map(
        [1, 2, 3],
        concurrency=2,
        context={"seed": 0},
        return_exceptions=True,
    )
    assert out[0] == 1
    assert isinstance(out[1], StageExecutionError)
    assert out[2] == 3
