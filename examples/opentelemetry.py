"""Optional OpenTelemetry middleware (install async-pipeline[otel])."""

from __future__ import annotations

import asyncio

from async_pipeline import Pipeline, Stage


async def main() -> None:
    try:
        from async_pipeline.telemetry import OpenTelemetryMiddleware
    except ImportError:
        print("Skip: install async-pipeline[otel] to run this example.")
        return

    async def step(x: int) -> int:
        return x + 1

    pipeline = Pipeline(
        [Stage("step", step)],
        middlewares=[OpenTelemetryMiddleware()],
    )
    print(await pipeline.run(1))


if __name__ == "__main__":
    asyncio.run(main())
