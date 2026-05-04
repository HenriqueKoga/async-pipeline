"""Timing middleware: record per-stage durations in the execution context."""

import time
from collections.abc import Awaitable, Callable
from typing import Any


class TimingMiddleware:
    """Record wall time for each ``await next(value)`` in the execution context.

    Durations (seconds from :func:`time.perf_counter`) are appended to a list
    per stage name so repeated names do not overwrite. Timing runs in a
    ``finally`` block so failures still record duration.
    """

    def __init__(self, context_key: str = "timings") -> None:
        """Args:
            context_key: Dict key under which per-stage lists are stored.
        """
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
