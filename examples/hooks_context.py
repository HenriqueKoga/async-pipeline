"""Lifecycle hooks and shared execution context."""

from __future__ import annotations

import asyncio

from async_pipeline import Pipeline, Stage


def before_stage(name: str, value: object, ctx: dict[str, object]) -> None:
    ctx.setdefault("events", []).append(f"before:{name}:{value!r}")


def after_stage(
    name: str,
    value: object,
    output: object | None,
    error: Exception | None,
    ctx: dict[str, object],
) -> None:
    ctx.setdefault("events", []).append(f"after:{name}:err={error is not None}")


async def add_tag(value: int, context: dict[str, object]) -> int:
    context["last"] = value
    return value + 1


async def main() -> None:
    ctx: dict[str, object] = {}
    pipeline = Pipeline(
        [Stage("add_tag", add_tag)],
        before_stage=before_stage,
        after_stage=after_stage,
    )
    out = await pipeline.run(40, context=ctx)
    print("result:", out)
    print("context:", ctx)


if __name__ == "__main__":
    asyncio.run(main())
