"""Optional OpenTelemetry integration (install ``async-pipeline[otel]``)."""

from __future__ import annotations

import importlib
from typing import Any

__all__ = ["OpenTelemetryMiddleware"]

_OTEL_MSG = "OpenTelemetry support requires installing async-pipeline[otel]"


def __getattr__(name: str) -> Any:
    if name != "OpenTelemetryMiddleware":
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    try:
        mod = importlib.import_module("async_pipeline.telemetry.opentelemetry")
    except ImportError as exc:
        raise ImportError(_OTEL_MSG) from exc
    return getattr(mod, "OpenTelemetryMiddleware")
