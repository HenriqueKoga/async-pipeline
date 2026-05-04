"""Official built-in middlewares (logging + timing)."""

from __future__ import annotations

import asyncio

from async_pipeline import Pipeline, Stage
from async_pipeline.middlewares import LoggingMiddleware, TimingMiddleware


async def work(value: int) -> int:
    return value + 7


async def main() -> None:
    ctx: dict[str, object] = {}
    pipeline = Pipeline(
        [Stage("work", work)],
        middlewares=[LoggingMiddleware(), TimingMiddleware()],
    )
    print(await pipeline.run(1, context=ctx))
    print("timings:", ctx.get("timings"))


if __name__ == "__main__":
    asyncio.run(main())
