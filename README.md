# async-pipeline

[![PyPI version](https://img.shields.io/pypi/v/async-pipeline)](https://pypi.org/project/async-pipeline/)
[![Python versions](https://img.shields.io/pypi/pyversions/async-pipeline)](https://pypi.org/project/async-pipeline/)
[![CI](https://github.com/HenriqueKoga/async-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/HenriqueKoga/async-pipeline/actions/workflows/ci.yml)
[![Release](https://github.com/HenriqueKoga/async-pipeline/actions/workflows/release.yml/badge.svg)](https://github.com/HenriqueKoga/async-pipeline/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Typing: typed](https://img.shields.io/pypi/types/async-pipeline)](https://pypi.org/project/async-pipeline/)

Composable **async pipelines** for Python 3.14+: run **stages** in sequence, share **context**, add **hooks** and **middleware**, batch with **`Pipeline.map`**, and tune **retry** / **timeout** per stage.

## Overview

- **Pipeline** — ordered list of **Stage** handlers; each output feeds the next input.
- **Execution context** — optional mutable `dict` shared by stages and hooks for one run (or a shallow copy per item in **`map`**).
- **Hooks** — lightweight `before_stage` / `after_stage` observers (errors inside hooks are ignored by design).
- **Middleware** — data-path wrappers around each `Stage.run` (logging, timing, retries, timeouts, OpenTelemetry, …).
- **Typing** — PEP 695 generics on `Pipeline` / `Stage`, `mypy --strict` clean, `py.typed` included.

Public API: `async_pipeline`, `async_pipeline.middlewares`, `async_pipeline.telemetry` (optional OTEL extra).

## Installation

### uv

```bash
uv add async-pipeline
```

### pip

```bash
pip install async-pipeline
```

### OpenTelemetry extra

Tracing middleware is optional:

```bash
uv add "async-pipeline[otel]"
# or
pip install "async-pipeline[otel]"
```

## Quickstart

```python
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
    assert await pipeline.run(10) == 22


asyncio.run(main())
```

Synchronous handlers are allowed (`Stage.run` remains async):

```python
def add_one(value: int) -> int:
    return value + 1


pipeline = Pipeline([Stage("add_one", add_one)])
```

## Core concepts

### Pipeline

`Pipeline(stages=[...], *, before_stage=..., after_stage=..., middlewares=...)` runs stages in order. The first middleware in the list is the **outermost** wrapper (see [Middleware order](#middleware-order)).

### Stage

`Stage(name, handler, *, timeout=..., retries=..., retry_delay=..., backoff=...)`. Failures surface as `StageExecutionError` after handler retries are exhausted. Invalid constructor arguments raise `ValueError`.

### Execution context

Pass `context=` to `Pipeline.run` or `Pipeline.map`. If omitted, `run` uses a new empty `dict` for that execution. Handlers may use `(value)` or `(value, context)` when the callable accepts at least two positional parameters (detected at `Stage` construction). Hooks may take an extra `context` argument using the same arity rules.

## Batch processing with `Pipeline.map`

```python
results = await pipeline.map([1, 2, 3], concurrency=5)
```

- Uses **`asyncio.TaskGroup`** and a semaphore for bounded concurrency.
- Output list matches input order.
- Optional `context=` is shallow-copied per item for safe parallelism.
- `return_exceptions=True` stores failures in the result list instead of raising `ExceptionGroup`.

## Error handling

Handler failures become **`StageExecutionError`** with **`stage_name`** and **`original_error`**. An empty `stages` sequence raises **`ValueError`** at pipeline construction.

```python
from async_pipeline import Pipeline, Stage, StageExecutionError

async def broken(value: int) -> int:
    raise RuntimeError("boom")


pipeline = Pipeline([Stage("broken", broken)])

try:
    await pipeline.run(1)
except StageExecutionError as exc:
    assert exc.stage_name == "broken"
    assert isinstance(exc.original_error, RuntimeError)
```

## Timeout and retry on `Stage`

- **`timeout`** — applied with **`asyncio.timeout`** around awaitable handlers only.
- **`retries`**, **`retry_delay`**, **`backoff`** (`"fixed"` | `"exponential"`) — total attempts = **`1 + retries`**.
- **`KeyboardInterrupt`** and **`asyncio.CancelledError`** are not retried.

See **`examples/retry_timeout.py`**.

## Hooks

`before_stage` runs immediately before the middleware chain + `Stage.run`; `after_stage` runs after completion (success or failure). Hook callables may be sync or async. Hook failures do not stop the pipeline or mask stage errors.

## Middlewares

Each middleware is an async callable:

```text
async def mw(next, stage_name, value, context) -> Any:
    return await next(value)
```

Or a class with `async def __call__(self, next, stage_name, value, context)`.

Unlike hooks, middleware participates in the data path (transform values, enforce policies).

## Built-in middlewares

```python
from async_pipeline.middlewares import (
    LoggingMiddleware,
    RetryMiddleware,
    TimeoutMiddleware,
    TimingMiddleware,
)
```

### `LoggingMiddleware`

```python
import logging

pipeline = Pipeline(
    [...],
    middlewares=[LoggingMiddleware(logger=logging.getLogger("app"))],
)
```

### `TimingMiddleware`

```python
context = {}
pipeline = Pipeline(
    [...],
    middlewares=[TimingMiddleware()],
)
result = await pipeline.run(1, context=context)
print(context["timings"])
```

### `RetryMiddleware`

Middleware-level retries in addition to `Stage(retries=...)`.

```python
pipeline = Pipeline(
    [...],
    middlewares=[
        RetryMiddleware(
            retries=3,
            delay=0.5,
            backoff="exponential",
        ),
    ],
)
```

### `TimeoutMiddleware`

```python
pipeline = Pipeline(
    [...],
    middlewares=[TimeoutMiddleware(timeout=5.0)],
)
```

### Middleware order

List order is **outside → in** toward the stage, matching execution: first middleware runs first around the rest of the chain.

```python
middlewares=[
    LoggingMiddleware(),
    TimingMiddleware(),
    RetryMiddleware(retries=3),
    TimeoutMiddleware(timeout=5.0),
]
```

## OpenTelemetry

Optional tracing (install **`async-pipeline[otel]`**):

```python
from async_pipeline import Pipeline, Stage
from async_pipeline.telemetry import OpenTelemetryMiddleware

pipeline = Pipeline(
    [...],
    middlewares=[OpenTelemetryMiddleware()],
)
```

Custom span attributes from context:

```python
await pipeline.run(
    1,
    context={
        "trace_attributes": {
            "request_id": "abc-123",
        }
    },
)
```

## Examples

Runnable scripts live under **`examples/`** (see **[examples/README.md](examples/README.md)**):

```bash
uv run python examples/basic_pipeline.py
uv run python examples/batch_processing.py
```

## Development

Clone the repo, then:

```bash
uv sync --extra otel
uv run pytest
uv run ruff check .
uv run mypy src
uv build
uv run twine check dist/*
```

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for branch conventions, PR flow, and maintainer release steps.

## Release process

On each push to **`master`**, [`.github/workflows/release.yml`](.github/workflows/release.yml):

1. Reads **`version`** from **`pyproject.toml`**.
2. Skips publish if git tag **`v<version>`** already exists.
3. Runs tests, lint, typecheck, **`uv build`**, **`twine check`**.
4. Publishes to **PyPI** with **Trusted Publishing** (OIDC).
5. Creates and pushes the **`v<version>`** tag.

Configure the PyPI project to trust this GitHub repository (see PyPI trusted publishing documentation).

## Versioning policy

[Semantic Versioning](https://semver.org/). Public exports from **`async_pipeline`**, **`async_pipeline.middlewares`**, and **`async_pipeline.telemetry`** follow semver for **1.x** unless called out in the changelog.

## Changelog

Release history: **[CHANGELOG.md](CHANGELOG.md)**.

## License

This project is licensed under the MIT License — see **[LICENSE](LICENSE)**.
