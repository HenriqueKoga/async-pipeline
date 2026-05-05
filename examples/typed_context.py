"""Typed context with dataclass, hooks, and Pipeline.run."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from async_pipeline import Pipeline, Stage


@dataclass
class MyContext:
    request_id: str
    count: int = 0


def before_stage(name: str, value: object, context: MyContext) -> None:
    context.count += 1
    print(f"[before] {name} request_id={context.request_id} value={value!r}")


async def handler(value: int, context: MyContext) -> int:
    context.count += 1
    return value + context.count


async def main() -> None:
    ctx = MyContext(request_id="abc-123")
    pipeline: Pipeline[int, int, MyContext] = Pipeline(
        [Stage("handler", handler)],
        before_stage=before_stage,
    )
    result = await pipeline.run(1, context=ctx)
    print("result:", result)
    print("context:", ctx)


if __name__ == "__main__":
    asyncio.run(main())
