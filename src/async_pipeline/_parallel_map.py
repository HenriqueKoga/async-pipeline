"""Bounded-concurrency ``map`` with stable output order."""

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, cast


async def map_ordered[T_item, T_result](
    items: Iterable[T_item],
    *,
    concurrency: int,
    return_exceptions: bool,
    template_context: dict[str, Any] | None,
    execute: Callable[[T_item, dict[str, Any]], Awaitable[T_result]],
) -> list[T_result] | list[T_result | Exception]:
    """Run ``execute`` per item with bounded concurrency; preserve input order."""
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    seq = list(items)
    if not seq:
        return []
    results: list[T_result | Exception | None] = [None] * len(seq)
    semaphore = asyncio.Semaphore(concurrency)
    template: dict[str, Any] = (
        {} if template_context is None else template_context
    )

    async def worker(index: int, item: T_item) -> None:
        async with semaphore:
            ctx = dict(template)
            try:
                results[index] = await execute(item, ctx)
            except Exception as exc:
                if not return_exceptions:
                    raise
                results[index] = exc

    async with asyncio.TaskGroup() as tg:
        for index, item in enumerate(seq):
            tg.create_task(worker(index, item))

    return cast(list[T_result] | list[T_result | Exception], results)
