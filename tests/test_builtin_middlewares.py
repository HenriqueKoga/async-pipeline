"""Tests for built-in pipeline middlewares."""

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError
from async_pipeline.middlewares import (
    LoggingMiddleware,
    RetryMiddleware,
    TimeoutMiddleware,
    TimingMiddleware,
)


async def test_logging_uses_default_logger(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="async_pipeline")

    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[LoggingMiddleware()],
    )
    assert await pipeline.run(1) == 2
    assert any("Starting stage: add_one" in r.message for r in caplog.records)
    assert any("Finished stage: add_one" in r.message for r in caplog.records)


async def test_logging_error_and_reraise(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="async_pipeline")

    async def boom(_: int) -> int:
        raise RuntimeError("x")

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("boom", boom)],
        middlewares=[LoggingMiddleware()],
    )
    with pytest.raises(StageExecutionError):
        await pipeline.run(0)
    assert any("Stage failed: boom" in r.message for r in caplog.records)


async def test_logging_include_value_false_no_payload_in_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="async_pipeline")

    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[LoggingMiddleware(include_value=False)],
    )
    await pipeline.run(99)
    texts = " ".join(r.message for r in caplog.records)
    assert "input=" not in texts
    assert "output=" not in texts


async def test_logging_include_value_true_shows_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="async_pipeline")

    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[LoggingMiddleware(include_value=True)],
    )
    await pipeline.run(5)
    texts = " ".join(r.message for r in caplog.records)
    assert "input=5" in texts
    assert "output=6" in texts


async def test_timing_middleware_adds_duration_to_context() -> None:
    async def add_one(value: int) -> int:
        return value + 1

    context: dict[str, Any] = {}
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[TimingMiddleware()],
    )
    result = await pipeline.run(1, context=context)
    assert result == 2
    assert "timings" in context
    assert "add_one" in context["timings"]
    assert isinstance(context["timings"]["add_one"], list)
    assert context["timings"]["add_one"][0] >= 0


async def test_timing_same_stage_name_appends_list() -> None:
    async def a(x: int) -> int:
        return x + 1

    async def b(x: int) -> int:
        return x * 2

    context: dict[str, Any] = {}
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("dup", a), Stage("dup", b)],
        middlewares=[TimingMiddleware()],
    )
    assert await pipeline.run(1, context=context) == 4
    assert len(context["timings"]["dup"]) == 2


async def test_timing_records_on_failure() -> None:
    async def boom(_: int) -> int:
        raise ValueError("nope")

    context: dict[str, Any] = {}
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("bad", boom)],
        middlewares=[TimingMiddleware()],
    )
    with pytest.raises(StageExecutionError):
        await pipeline.run(0, context=context)
    assert context["timings"]["bad"][0] >= 0


async def test_timing_with_map_isolated_context() -> None:
    async def inc(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("inc", inc)],
        middlewares=[TimingMiddleware()],
    )
    template: dict[str, Any] = {"shared": True}
    results = await pipeline.map([1, 2], concurrency=2, context=template)
    assert results == [2, 3]
    assert template.get("timings") is None


async def test_retry_middleware_retries_until_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    async def flaky(value: int) -> int:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise RuntimeError("temporary failure")
        return value + 1

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("flaky", flaky)],
        middlewares=[RetryMiddleware(retries=2)],
    )
    result = await pipeline.run(1)
    assert result == 2
    assert calls == 2
    assert sleeps == []


async def test_retry_exhausted_raises_last_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def always_fail(_: int) -> int:
        raise RuntimeError("bad")

    monkeypatch.setattr(asyncio, "sleep", lambda _d: asyncio.sleep(0))
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("x", always_fail)],
        middlewares=[RetryMiddleware(retries=1, delay=0.0)],
    )
    with pytest.raises(StageExecutionError) as excinfo:
        await pipeline.run(0)
    assert isinstance(excinfo.value.__cause__, RuntimeError)


async def test_retry_zero_no_extra_attempts() -> None:
    calls = 0

    async def once_fail(_: int) -> int:
        nonlocal calls
        calls += 1
        raise RuntimeError("once")

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("x", once_fail)],
        middlewares=[RetryMiddleware(retries=0)],
    )
    with pytest.raises(StageExecutionError):
        await pipeline.run(0)
    assert calls == 1


async def test_retry_backoff_fixed_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def fake_sleep(d: float) -> None:
        delays.append(d)

    async def fail(_: int) -> int:
        raise RuntimeError("x")

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("f", fail)],
        middlewares=[RetryMiddleware(retries=2, delay=0.5, backoff="fixed")],
    )
    with pytest.raises(StageExecutionError):
        await pipeline.run(0)
    assert delays == [0.5, 0.5]


async def test_retry_backoff_exponential_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []

    async def fake_sleep(d: float) -> None:
        delays.append(d)

    async def fail(_: int) -> int:
        raise RuntimeError("x")

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("f", fail)],
        middlewares=[RetryMiddleware(retries=3, delay=1.0, backoff="exponential")],
    )
    with pytest.raises(StageExecutionError):
        await pipeline.run(0)
    assert delays == [1.0, 2.0, 4.0]


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"retries": -1}, "retries must be greater than or equal to 0"),
        ({"delay": -0.1}, "delay must be greater than or equal to 0"),
    ],
)
def test_retry_invalid_params(kwargs: dict[str, Any], match: str) -> None:
    base: dict[str, Any] = {"retries": 1, "delay": 0.0, "backoff": "fixed"}
    base.update(kwargs)
    with pytest.raises(ValueError, match=match):
        RetryMiddleware(**base)


def test_retry_invalid_backoff() -> None:
    with pytest.raises(ValueError, match=r'backoff must be "fixed" or "exponential"'):
        RetryMiddleware(retries=0, delay=0.0, backoff="invalid")  # type: ignore[arg-type]


async def test_retry_no_retry_on_cancelled_error() -> None:
    calls = 0

    async def cancel_first(_: int) -> int:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError()

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("c", cancel_first)],
        middlewares=[RetryMiddleware(retries=3, delay=0.0)],
    )
    with pytest.raises(asyncio.CancelledError):
        await pipeline.run(0)
    assert calls == 1


async def test_retry_no_retry_on_keyboard_interrupt() -> None:
    calls = 0

    async def kb(_: int) -> int:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("kb", kb)],
        middlewares=[RetryMiddleware(retries=3, delay=0.0)],
    )
    with pytest.raises(KeyboardInterrupt):
        await pipeline.run(0)
    assert calls == 1


async def test_timeout_sufficient() -> None:
    async def fast(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("fast", fast)],
        middlewares=[TimeoutMiddleware(timeout=5.0)],
    )
    assert await pipeline.run(1) == 2


async def test_timeout_exceeded_raises_timeout_error() -> None:
    async def slow(_: int) -> int:
        await asyncio.sleep(10.0)
        return 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("slow", slow)],
        middlewares=[TimeoutMiddleware(timeout=0.05)],
    )
    with pytest.raises(TimeoutError):
        await pipeline.run(0)


def test_timeout_invalid() -> None:
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        TimeoutMiddleware(timeout=0)
    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        TimeoutMiddleware(timeout=-1.0)


def test_timeout_middleware_uses_asyncio_timeout_not_wait_for() -> None:
    text = inspect.getsource(TimeoutMiddleware)
    assert "asyncio.timeout" in text
    assert "wait_for" not in text


async def test_composition_logging_and_timing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="async_pipeline")

    async def add_one(x: int) -> int:
        return x + 1

    ctx: dict[str, Any] = {}
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[LoggingMiddleware(), TimingMiddleware()],
    )
    assert await pipeline.run(1, context=ctx) == 2
    assert "add_one" in ctx["timings"]
    assert any("Starting stage: add_one" in r.message for r in caplog.records)


async def test_composition_retry_and_timeout() -> None:
    """Both middlewares apply; inner timeout does not block a fast stage."""

    async def ok(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("ok", ok)],
        middlewares=[
            RetryMiddleware(retries=1, delay=0.0, backoff="fixed"),
            TimeoutMiddleware(timeout=10.0),
        ],
    )
    assert await pipeline.run(3) == 4


async def test_middleware_order_outer_first() -> None:
    events: list[str] = []

    async def outer_mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        events.append("outer-in")
        r = await next_fn(v)
        events.append("outer-out")
        return r

    async def inner_mw(
        next_fn: Callable[[Any], Awaitable[Any]],
        _n: str,
        v: Any,
        _c: dict[str, Any],
    ) -> Any:
        events.append("inner-in")
        r = await next_fn(v)
        events.append("inner-out")
        return r

    async def ident(x: int) -> int:
        return x

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("ident", ident)],
        middlewares=[outer_mw, inner_mw],
    )
    await pipeline.run(1)
    assert events == ["outer-in", "inner-in", "inner-out", "outer-out"]


async def test_builtin_with_hooks() -> None:
    seen: list[str] = []

    def before(n: str, _v: object) -> None:
        seen.append(f"before:{n}")

    def after(
        n: str,
        _v: object,
        _o: object | None,
        _e: Exception | None,
    ) -> None:
        seen.append(f"after:{n}")

    async def add_one(x: int) -> int:
        return x + 1

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        before_stage=before,
        after_stage=after,
        middlewares=[TimingMiddleware()],
    )
    ctx: dict[str, Any] = {}
    await pipeline.run(0, context=ctx)
    assert seen == ["before:add_one", "after:add_one"]
    assert "add_one" in ctx["timings"]


async def test_builtin_with_map_return_exceptions() -> None:
    async def maybe(x: int) -> int:
        if x == 2:
            raise RuntimeError("two")
        return x

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("m", maybe)],
        middlewares=[TimingMiddleware()],
    )
    out = await pipeline.map([1, 2, 3], concurrency=2, return_exceptions=True)
    assert out[0] == 1
    assert isinstance(out[1], StageExecutionError)
    assert out[2] == 3
