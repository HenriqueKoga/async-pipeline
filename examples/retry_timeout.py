"""Per-stage retry and asyncio-based timeout."""

from __future__ import annotations

import asyncio

from async_pipeline import Pipeline, Stage


async def slow(value: int) -> int:
    await asyncio.sleep(0.01)
    return value + 1


async def main() -> None:
    pipeline = Pipeline(
        [
            Stage("slow", slow, timeout=0.5, retries=1, retry_delay=0.0),
        ],
    )
    print(await pipeline.run(1))


if __name__ == "__main__":
    asyncio.run(main())
