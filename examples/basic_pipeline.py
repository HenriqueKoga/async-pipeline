"""Minimal async pipeline: two stages, one run."""

from __future__ import annotations

import asyncio

from async_pipeline import Pipeline, Stage


async def add_one(value: int) -> int:
    return value + 1


async def multiply_by_two(value: int) -> int:
    return value * 2


async def main() -> None:
    pipeline = Pipeline(
        [
            Stage("add_one", add_one),
            Stage("multiply_by_two", multiply_by_two),
        ],
    )
    result = await pipeline.run(10)
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
