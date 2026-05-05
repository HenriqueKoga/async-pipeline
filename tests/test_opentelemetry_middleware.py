"""OpenTelemetry middleware behavior (tracer/span mocked; no exporter)."""

from unittest.mock import MagicMock

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError
from async_pipeline.telemetry.opentelemetry import OpenTelemetryMiddleware


@pytest.fixture
def tracer_and_span(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    span = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=span)
    ctx.__exit__ = MagicMock(return_value=False)
    tracer = MagicMock()
    tracer.start_as_current_span.return_value = ctx
    monkeypatch.setattr(
        "async_pipeline.telemetry.opentelemetry.trace.get_tracer",
        lambda _name: tracer,
    )
    return tracer, span


async def test_core_pipeline_import_unrelated_to_otel() -> None:
    """Core symbols used by this module stay importable (no telemetry import)."""
    assert Pipeline is not None
    assert Stage is not None


async def test_middleware_runs_pipeline(
    tracer_and_span: tuple[MagicMock, MagicMock],
) -> None:
    tracer, span = tracer_and_span

    async def add_one(x: int) -> int:
        return x + 1

    mw = OpenTelemetryMiddleware()
    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("add_one", add_one)],
        middlewares=[mw],
    )
    assert await pipeline.run(1) == 2
    tracer.start_as_current_span.assert_called_once_with("pipeline.stage.add_one")
    span.set_attribute.assert_any_call("async_pipeline.stage.name", "add_one")
    span.set_attribute.assert_any_call("async_pipeline.input.type", "int")
    span.set_attribute.assert_any_call("async_pipeline.output.type", "int")


async def test_middleware_propagates_stage_error(
    tracer_and_span: tuple[MagicMock, MagicMock],
) -> None:
    _tracer, span = tracer_and_span

    async def boom(_: int) -> int:
        raise RuntimeError("nope")

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("boom", boom)],
        middlewares=[OpenTelemetryMiddleware()],
    )
    with pytest.raises(StageExecutionError):
        await pipeline.run(0)
    span.record_exception.assert_called_once()
    span.set_status.assert_called()
    err_calls = [
        c
        for c in span.set_attribute.call_args_list
        if c[0][0] == "async_pipeline.error"
    ]
    assert err_calls


async def test_trace_attributes_merged(
    tracer_and_span: tuple[MagicMock, MagicMock],
) -> None:
    _tracer, span = tracer_and_span

    async def ident(x: int) -> int:
        return x

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("ident", ident)],
        middlewares=[OpenTelemetryMiddleware()],
    )
    ctx = {
        "trace_attributes": {
            "request_id": "abc-123",
            "tenant_id": "t1",
            "skip": {"nested": 1},
            "n": 42,
            "f": 1.5,
            "b": True,
        }
    }
    assert await pipeline.run(7, context=ctx) == 7
    span.set_attribute.assert_any_call("request_id", "abc-123")
    span.set_attribute.assert_any_call("tenant_id", "t1")
    span.set_attribute.assert_any_call("n", 42)
    span.set_attribute.assert_any_call("f", 1.5)
    span.set_attribute.assert_any_call("b", True)
    keys = {c[0][0] for c in span.set_attribute.call_args_list}
    assert "skip" not in keys


async def test_trace_attributes_merged_from_object(
    tracer_and_span: tuple[MagicMock, MagicMock],
) -> None:
    _tracer, span = tracer_and_span

    class ContextObj:
        def __init__(self) -> None:
            self.trace_attributes = {
                "request_id": "obj-123",
                "tries": 2,
                "invalid": {"x": 1},
            }

    async def ident(x: int) -> int:
        return x

    pipeline: Pipeline[int, int, ContextObj] = Pipeline(
        [Stage("ident", ident)],
        middlewares=[OpenTelemetryMiddleware()],
    )
    assert await pipeline.run(7, context=ContextObj()) == 7
    span.set_attribute.assert_any_call("request_id", "obj-123")
    span.set_attribute.assert_any_call("tries", 2)
    keys = {c[0][0] for c in span.set_attribute.call_args_list}
    assert "invalid" not in keys


async def test_middleware_with_map(
    tracer_and_span: tuple[MagicMock, MagicMock],
) -> None:
    _tracer, span = tracer_and_span

    async def double(x: int) -> int:
        return x * 2

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("double", double)],
        middlewares=[OpenTelemetryMiddleware()],
    )
    assert await pipeline.map([1, 2], concurrency=2) == [2, 4]
    assert span.set_attribute.call_count >= 6


async def test_custom_span_prefix_and_tracer_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    span = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=span)
    ctx.__exit__ = MagicMock(return_value=False)
    tracer = MagicMock()
    tracer.start_as_current_span.return_value = ctx
    get = MagicMock(return_value=tracer)
    monkeypatch.setattr(
        "async_pipeline.telemetry.opentelemetry.trace.get_tracer",
        get,
    )

    async def ident(x: int) -> int:
        return x

    pipeline: Pipeline[int, int] = Pipeline(
        [Stage("x", ident)],
        middlewares=[
            OpenTelemetryMiddleware(tracer_name="my.tracer", span_prefix="app.pipe")
        ],
    )
    await pipeline.run(3)
    get.assert_called_once_with("my.tracer")
    tracer.start_as_current_span.assert_called_once_with("app.pipe.x")
