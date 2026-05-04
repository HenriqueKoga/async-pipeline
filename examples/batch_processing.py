"""Run the same pipeline for many inputs with bounded concurrency."""

from __future__ import annotations

import asyncio

from async_pipeline import Pipeline, Stage


async def double(value: int) -> int:
    return value * 2


async def main() -> None:
    pipeline = Pipeline([Stage("double", double)])
    results = await pipeline.map([1, 2, 3, 4], concurrency=2)
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
