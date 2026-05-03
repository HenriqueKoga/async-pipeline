"""Tests for Pipeline before_stage / after_stage hooks."""

import asyncio

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError


async def test_before_stage_sync_called_per_stage() -> None:
    calls: list[tuple[str, object]] = []

    async def add_one(value: int) -> int:
        return value + 1

    async def mul_two(value: int) -> int:
        return value * 2

    def before_stage(stage_name: str, input_value: object) -> None:
        calls.append((stage_name, input_value))

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one), Stage("mul_two", mul_two)],
        before_stage=before_stage,
    )
    assert await pipeline.run(1) == 4
    assert calls == [("add_one", 1), ("mul_two", 2)]


async def test_after_stage_sync_on_success() -> None:
    calls: list[tuple[str, object, object | None, Exception | None]] = []

    async def add_one(value: int) -> int:
        return value + 1

    def after_stage(
        stage_name: str,
        input_value: object,
        output_value: object | None,
        error: Exception | None,
    ) -> None:
        calls.append((stage_name, input_value, output_value, error))

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        after_stage=after_stage,
    )
    assert await pipeline.run(5) == 6
    assert len(calls) == 1
    name, inp, out, err = calls[0]
    assert name == "add_one"
    assert inp == 5
    assert out == 6
    assert err is None


async def test_after_stage_sync_on_stage_error() -> None:
    calls: list[tuple[str, object, object | None, Exception | None]] = []

    async def boom(value: int) -> int:
        raise RuntimeError("boom")

    def after_stage(
        stage_name: str,
        input_value: object,
        output_value: object | None,
        error: Exception | None,
    ) -> None:
        calls.append((stage_name, input_value, output_value, error))

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("bad", boom)],
        after_stage=after_stage,
    )
    with pytest.raises(StageExecutionError) as exc:
        await pipeline.run(1)
    assert len(calls) == 1
    name, inp, out, err = calls[0]
    assert name == "bad"
    assert inp == 1
    assert out is None
    assert isinstance(err, StageExecutionError)
    assert isinstance(exc.value, StageExecutionError)


async def test_hooks_async_awaited() -> None:
    before_done = asyncio.Event()
    after_done = asyncio.Event()

    async def add_one(x: int) -> int:
        return x + 1

    async def before_stage(stage_name: str, input_value: object) -> None:
        assert stage_name == "add_one"
        assert input_value == 2
        before_done.set()

    async def after_stage(
        stage_name: str,
        input_value: object,
        output_value: object | None,
        error: Exception | None,
    ) -> None:
        assert stage_name == "add_one"
        assert input_value == 2
        assert output_value == 3
        assert error is None
        after_done.set()

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        before_stage=before_stage,
        after_stage=after_stage,
    )
    assert await pipeline.run(2) == 3
    assert before_done.is_set()
    assert after_done.is_set()


async def test_before_stage_error_does_not_break_pipeline() -> None:
    stage_calls = 0

    async def add_one(x: int) -> int:
        nonlocal stage_calls
        stage_calls += 1
        return x + 1

    def before_stage(_stage_name: str, _input_value: object) -> None:
        raise RuntimeError("hook failed")

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        before_stage=before_stage,
    )
    assert await pipeline.run(0) == 1
    assert stage_calls == 1


async def test_after_stage_error_does_not_break_success() -> None:
    async def add_one(x: int) -> int:
        return x + 1

    def after_stage(
        _stage_name: str,
        _input_value: object,
        _output_value: object | None,
        _error: Exception | None,
    ) -> None:
        raise RuntimeError("after failed")

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        after_stage=after_stage,
    )
    assert await pipeline.run(4) == 5


async def test_after_stage_error_does_not_replace_stage_error() -> None:
    async def bad(_: int) -> int:
        raise ValueError("from stage")

    def after_stage(
        _stage_name: str,
        _input_value: object,
        _output_value: object | None,
        _error: Exception | None,
    ) -> None:
        raise RuntimeError("from hook")

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("s", bad)],
        after_stage=after_stage,
    )
    with pytest.raises(StageExecutionError) as exc:
        await pipeline.run(0)
    assert isinstance(exc.value.original_error, ValueError)


async def test_hooks_with_map_concurrent() -> None:
    lock = asyncio.Lock()
    before_counts: dict[str, int] = {}
    after_counts: dict[str, int] = {}

    async def add_one(x: int) -> int:
        return x + 1

    async def mul_two(x: int) -> int:
        return x * 2

    async def before_stage(stage_name: str, _input_value: object) -> None:
        async with lock:
            before_counts[stage_name] = before_counts.get(stage_name, 0) + 1

    async def after_stage(
        stage_name: str,
        _input_value: object,
        _output_value: object | None,
        error: Exception | None,
    ) -> None:
        assert error is None
        async with lock:
            after_counts[stage_name] = after_counts.get(stage_name, 0) + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one), Stage("mul_two", mul_two)],
        before_stage=before_stage,
        after_stage=after_stage,
    )
    out = await pipeline.map([1, 2, 3], concurrency=2)
    assert out == [4, 6, 8]
    assert before_counts == {"add_one": 3, "mul_two": 3}
    assert after_counts == {"add_one": 3, "mul_two": 3}


async def test_hooks_with_map_return_exceptions() -> None:
    lock = asyncio.Lock()
    error_after: list[Exception | None] = []

    async def fail_if_two(x: int) -> int:
        if x == 2:
            raise RuntimeError("no")
        return x

    async def after_stage(
        _stage_name: str,
        _input_value: object,
        _output_value: object | None,
        error: Exception | None,
    ) -> None:
        async with lock:
            error_after.append(error)

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("s", fail_if_two)],
        after_stage=after_stage,
    )
    results = await pipeline.map([1, 2, 3], concurrency=2, return_exceptions=True)
    assert results[0] == 1
    assert isinstance(results[1], StageExecutionError)
    assert results[2] == 3
    assert error_after.count(None) == 2
    assert sum(1 for e in error_after if e is not None) == 1
