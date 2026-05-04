"""Timing middleware: record per-stage durations in the execution context."""

import time
from collections.abc import Awaitable, Callable
from typing import Any


class TimingMiddleware:
    """Append each stage duration (seconds) to ``context[context_key][stage_name]``."""

    def __init__(self, context_key: str = "timings") -> None:
        self._context_key = context_key

    def _bucket_for_stage(
        self,
        context: dict[str, Any],
        stage_name: str,
    ) -> list[float]:
        root = context.setdefault(self._context_key, {})
        if not isinstance(root, dict):
            root = {}
            context[self._context_key] = root
        raw = root.get(stage_name)
        bucket: list[float]
        if not isinstance(raw, list):
            bucket = []
            root[stage_name] = bucket
        else:
            bucket = raw
        return bucket

    async def __call__(
        self,
        next_fn: Callable[[Any], Awaitable[Any]],
        stage_name: str,
        value: Any,
        context: dict[str, Any],
    ) -> Any:
        bucket = self._bucket_for_stage(context, stage_name)
        start = time.perf_counter()
        try:
            return await next_fn(value)
        finally:
            bucket.append(time.perf_counter() - start)
