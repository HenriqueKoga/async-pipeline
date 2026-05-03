"""Tests for Pipeline.map."""

import asyncio

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError


async def test_pipeline_map_basic() -> None:
    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add_one", add_one)])
    result = await pipeline.map([1, 2, 3], concurrency=2)
    assert result == [2, 3, 4]


async def test_pipeline_map_preserves_order() -> None:
    async def delayed_identity(x: int) -> int:
        # Later items finish first if we only looked at completion order.
        await asyncio.sleep(0.05 * (3 - x))
        return x * 10

    pipeline: Pipeline[int, int] = Pipeline([Stage("mul", delayed_identity)])
    out = await pipeline.map([0, 1, 2], concurrency=3)
    assert out == [0, 10, 20]


async def test_pipeline_map_respects_concurrency() -> None:
    active = 0
    max_active = 0
    lock = asyncio.Lock()

    async def slow_add(x: int) -> int:
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
        await asyncio.sleep(0.02)
        async with lock:
            active -= 1
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("slow_add", slow_add)])
    await pipeline.map(list(range(6)), concurrency=2)
    assert max_active <= 2


async def test_pipeline_map_error_raises_exception_group() -> None:
    async def fail_on_two(x: int) -> int:
        if x == 2:
            raise RuntimeError("bad")
        return x

    pipeline: Pipeline[int, int] = Pipeline([Stage("maybe_fail", fail_on_two)])
    with pytest.raises(ExceptionGroup):
        await pipeline.map([1, 2, 3], concurrency=2)


async def test_pipeline_map_error_exception_group_contains_stage_error() -> None:
    async def fail_on_two(x: int) -> int:
        if x == 2:
            raise RuntimeError("bad")
        return x

    pipeline: Pipeline[int, int] = Pipeline([Stage("maybe_fail", fail_on_two)])
    with pytest.raises(ExceptionGroup) as exc_info:
        await pipeline.map([1, 2, 3], concurrency=2)
    assert any(isinstance(e, StageExecutionError) for e in exc_info.value.exceptions)


async def test_pipeline_map_return_exceptions_true() -> None:
    async def fail_on_two(x: int) -> int:
        if x == 2:
            raise RuntimeError("bad")
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("step", fail_on_two)])
    results = await pipeline.map([1, 2, 3], concurrency=2, return_exceptions=True)
    assert results[0] == 2
    assert isinstance(results[1], StageExecutionError)
    assert results[2] == 4


async def test_pipeline_map_empty_items() -> None:
    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add_one", add_one)])
    assert await pipeline.map([], concurrency=2) == []


async def test_pipeline_map_concurrency_below_one() -> None:
    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add_one", add_one)])
    with pytest.raises(ValueError, match="at least 1"):
        await pipeline.map([1], concurrency=0)


async def test_pipeline_map_propagates_stage_timeout() -> None:
    async def very_slow(_: int) -> int:
        await asyncio.sleep(1.0)
        return 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("very_slow", very_slow, timeout=0.05)]
    )
    with pytest.raises(ExceptionGroup) as exc_info:
        await pipeline.map([1, 2], concurrency=2)
    assert any(
        isinstance(e, StageExecutionError)
        and isinstance(e.original_error, TimeoutError)
        for e in exc_info.value.exceptions
    )


async def test_pipeline_map_return_exceptions_timeout() -> None:
    async def maybe_slow(x: int) -> int:
        if x == 2:
            await asyncio.sleep(0.2)
        await asyncio.sleep(0.01)
        return x

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("maybe_slow", maybe_slow, timeout=0.05)]
    )
    results = await pipeline.map([1, 2, 3], concurrency=2, return_exceptions=True)
    assert results[0] == 1
    assert isinstance(results[1], StageExecutionError)
    assert isinstance(results[1].original_error, TimeoutError)
    assert results[2] == 3
