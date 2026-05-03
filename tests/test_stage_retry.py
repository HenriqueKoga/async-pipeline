"""Tests for Stage retry and backoff."""

import asyncio

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError


async def test_retry_success_after_one_failure() -> None:
    calls = 0

    async def flaky(x: int) -> int:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise RuntimeError("fail")
        return x

    stage = Stage("flaky", flaky, retries=2)
    result = await stage.run(10)
    assert result == 10
    assert calls == 2


async def test_retry_exhausted_raises_stage_execution_error() -> None:
    async def always_fail(_: int) -> int:
        raise RuntimeError("always")

    stage = Stage("bad", always_fail, retries=1)
    with pytest.raises(StageExecutionError) as exc:
        await stage.run(0)
    assert isinstance(exc.value.original_error, RuntimeError)


async def test_retry_backoff_fixed_delays(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("async_pipeline.stage.asyncio.sleep", fake_sleep)

    calls = 0

    async def fail_twice(_: int) -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("nope")
        return 1

    stage = Stage("f", fail_twice, retries=3, retry_delay=0.25, backoff="fixed")
    assert await stage.run(0) == 1
    assert sleeps == [0.25, 0.25]


async def test_retry_exponential_backoff_delays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("async_pipeline.stage.asyncio.sleep", fake_sleep)

    calls = 0

    async def fail_twice(_: int) -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("nope")
        return 1

    stage = Stage(
        "f",
        fail_twice,
        retries=3,
        retry_delay=0.5,
        backoff="exponential",
    )
    assert await stage.run(0) == 1
    assert sleeps == [0.5, 1.0]


async def test_retry_with_timeout() -> None:
    calls = 0

    async def slow_then_fast(v: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(0.2)
        return v

    stage = Stage("x", slow_then_fast, timeout=0.1, retries=2, retry_delay=0.02)
    assert await stage.run(7) == 7
    assert calls == 2


async def test_retry_sync_handler() -> None:
    calls = 0

    def flaky_sync(x: int) -> int:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise ValueError("bad")
        return x + 1

    stage = Stage("sync", flaky_sync, retries=2)
    assert await stage.run(1) == 2
    assert calls == 2


async def test_retries_zero_no_extra_attempts() -> None:
    calls = 0

    async def once_fail(_: int) -> int:
        nonlocal calls
        calls += 1
        raise RuntimeError("fail")

    stage = Stage("x", once_fail, retries=0)
    with pytest.raises(StageExecutionError):
        await stage.run(0)
    assert calls == 1


def test_invalid_retries_negative() -> None:
    async def ok(_: int) -> int:
        return 1

    with pytest.raises(ValueError, match="retries must be greater than or equal to 0"):
        Stage("x", ok, retries=-1)  # type: ignore[arg-type]


def test_invalid_retry_delay_negative() -> None:
    async def ok(_: int) -> int:
        return 1

    with pytest.raises(
        ValueError,
        match="retry_delay must be greater than or equal to 0",
    ):
        Stage("x", ok, retry_delay=-0.1)


def test_invalid_backoff() -> None:
    async def ok(_: int) -> int:
        return 1

    with pytest.raises(ValueError, match='backoff must be "fixed" or "exponential"'):
        Stage("x", ok, backoff="linear")  # type: ignore[arg-type]


async def test_no_retry_on_cancelled_error() -> None:
    started = asyncio.Event()

    async def blocked(_: int) -> int:
        started.set()
        await asyncio.sleep(10)
        return 1

    stage = Stage("x", blocked, retries=5, retry_delay=0.01)
    task = asyncio.create_task(stage.run(0))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_pipeline_map_return_exceptions_with_retry() -> None:
    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline([Stage("add", add_one, retries=2)])
    out = await pipeline.map([1, 2], concurrency=2, return_exceptions=True)
    assert out == [2, 3]


async def test_pipeline_map_return_exceptions_retry_exhausted() -> None:
    async def boom(_: int) -> int:
        raise RuntimeError("e")

    pipeline: Pipeline[int, int] = Pipeline([Stage("b", boom, retries=1)])
    results = await pipeline.map([1], concurrency=1, return_exceptions=True)
    assert len(results) == 1
    assert isinstance(results[0], StageExecutionError)


async def test_retry_delay_zero_skips_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[bool] = []

    async def no_sleep(_delay: float) -> None:
        called.append(True)

    monkeypatch.setattr("async_pipeline.stage.asyncio.sleep", no_sleep)

    c = 0

    async def fail_once(_: int) -> int:
        nonlocal c
        c += 1
        if c < 2:
            raise RuntimeError("n")
        return 1

    stage = Stage("f", fail_once, retries=2, retry_delay=0.0, backoff="fixed")
    assert await stage.run(0) == 1
    assert called == []
