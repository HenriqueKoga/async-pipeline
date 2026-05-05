"""OpenTelemetry tracing middleware (requires ``async-pipeline[otel]``)."""

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from async_pipeline._context import get_context_value

try:
    from opentelemetry.trace import Status, StatusCode

    from opentelemetry import trace
except ImportError as exc:
    raise ImportError(
        "OpenTelemetry support requires installing async-pipeline[otel]"
    ) from exc

_SIMPLE_ATTR_TYPES = (str, int, float, bool)


def _merge_trace_attributes(span: Any, context: object) -> None:
    raw = get_context_value(context, "trace_attributes", None)
    if not isinstance(raw, Mapping):
        return
    for key, val in raw.items():
        if isinstance(key, str) and isinstance(val, _SIMPLE_ATTR_TYPES):
            span.set_attribute(key, val)


class OpenTelemetryMiddleware:
    """Emit one OpenTelemetry span per stage around ``await next(value)``.

    Span name is ``"{span_prefix}.{stage_name}"`` (interpolated at runtime).
    Adds semantic attributes
    and merges simple values from ``context[\"trace_attributes\"]`` when
    present. Records failures on the span then re-raises the same exception.

    Requires the ``async-pipeline[otel]`` extra (see package metadata).
    """

    def __init__(
        self,
        tracer_name: str = "async_pipeline",
        span_prefix: str = "pipeline.stage",
    ) -> None:
        """Args:
            tracer_name: Name passed to :func:`trace.get_tracer`.
            span_prefix: Prefix joined with ``stage_name`` for the span name.
        """
        self._tracer = trace.get_tracer(tracer_name)
        self._span_prefix = span_prefix

    async def __call__(
        self,
        next_fn: Callable[[Any], Awaitable[Any]],
        stage_name: str,
        value: Any,
        context: object,
    ) -> Any:
        span_name = f"{self._span_prefix}.{stage_name}"
        with self._tracer.start_as_current_span(span_name) as span:
            span.set_attribute("async_pipeline.stage.name", stage_name)
            span.set_attribute("async_pipeline.input.type", type(value).__name__)
            _merge_trace_attributes(span, context)
            try:
                result = await next_fn(value)
            except Exception as exc:
                span.set_attribute("async_pipeline.error", True)
                span.set_attribute("async_pipeline.error.type", type(exc).__name__)
                span.set_attribute("async_pipeline.error.message", str(exc))
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise
            span.set_attribute("async_pipeline.output.type", type(result).__name__)
            span.set_status(Status(StatusCode.OK))
            return result
