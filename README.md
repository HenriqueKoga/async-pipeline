# async-pipeline

[![PyPI version](https://img.shields.io/pypi/v/async-pipeline)](https://pypi.org/project/async-pipeline/)
[![Python versions](https://img.shields.io/pypi/pyversions/async-pipeline)](https://pypi.org/project/async-pipeline/)
[![CI](https://github.com/HenriqueKoga/async-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/HenriqueKoga/async-pipeline/actions/workflows/ci.yml)

A small library for composing **async pipelines**: each `Stage` receives the previous stage’s output, executed in order.

## Requirements

- Python 3.14 or newer

## Install with uv

In your project:

```bash
uv add async-pipeline
```

To work on this library:

```bash
git clone <repo-url>
cd async-pipeline
uv sync
```

## Basic usage

```python
from async_pipeline import Pipeline, Stage

async def add_one(value: int) -> int:
    return value + 1

async def multiply_by_two(value: int) -> int:
    return value * 2

pipeline = Pipeline([
    Stage("add_one", add_one),
    Stage("multiply_by_two", multiply_by_two),
])

result = await pipeline.run(10)
assert result == 22
```

**Synchronous** handlers are supported as well (the stage’s `run` method remains `async`):

```python
def add_one(value: int) -> int:
    return value + 1

pipeline = Pipeline([
    Stage("add_one", add_one),
])

result = await pipeline.run(1)
assert result == 2
```

## Errors

Failures inside a handler are surfaced as `StageExecutionError`, including the stage name and the original exception:

```python
from async_pipeline import Pipeline, Stage, StageExecutionError

async def broken(value: int) -> int:
    raise RuntimeError("boom")

pipeline = Pipeline([
    Stage("broken", broken),
])

try:
    await pipeline.run(1)
except StageExecutionError as exc:
    assert exc.stage_name == "broken"
    assert isinstance(exc.original_error, RuntimeError)
```

A `Pipeline` with no stages raises `ValueError` at construction time.

## Stage timeout

Per-stage timeouts apply only when the handler returns an **awaitable** (async handler). Synchronous handlers are unchanged; the `timeout` argument is ignored for them.

Timeouts use **`asyncio.timeout`** (not `wait_for`). If the awaitable runs longer than the limit, the stage raises **`StageExecutionError`** with **`original_error`** set to **`TimeoutError`**.

```python
import asyncio

from async_pipeline import Pipeline, Stage

async def fetch_data(value: int) -> int:
    await asyncio.sleep(2)
    return value

pipeline = Pipeline([
    Stage("fetch_data", fetch_data, timeout=1.0),
])
```

Invalid values (`timeout <= 0`) raise **`ValueError`** with message `timeout must be greater than 0`.

## Retry

Configure automatic re-runs when a handler raises an **`Exception`** (including **`TimeoutError`** from **`asyncio.timeout`**). On success, the stage returns immediately. If every attempt fails, the library raises **`StageExecutionError`** with the **last** exception as **`original_error`**.

- **`retries`** — extra attempts after the first (`retries=0` is the default: no retries). Total tries are **`1 + retries`**.
- **`retry_delay`** — base seconds to wait after a failed attempt before the next one. If **`0`**, no **`asyncio.sleep`** is used between attempts.
- **`backoff`** — **`"fixed"`** (same delay after each failure) or **`"exponential"`** (delay multiplies by **`2 ** (failure_number - 1)`** from the base **`retry_delay`**, e.g. `0.5s`, `1.0s`, `2.0s`, …).

**`CancelledError`** and **`KeyboardInterrupt`** are **`BaseException`**, not **`Exception`**, so they are **not** retried and propagate as usual.

```python
from async_pipeline import Stage

stage = Stage(
    "api_call",
    api_call,
    retries=3,
    retry_delay=0.5,
    backoff="exponential",
    timeout=5.0,
)
```

**With timeout:** each attempt is wrapped in **`asyncio.timeout`** when **`timeout`** is set, so a slow await can fail with **`TimeoutError`**, trigger a retry (after the backoff sleep), and eventually surface as **`StageExecutionError`** if all tries time out or fail.

Invalid **`retries`**, **`retry_delay`**, or **`backoff`** values raise **`ValueError`** with a clear message.

## Batch processing

Run the same pipeline for many inputs in parallel, with a fixed concurrency limit and **stable output order** (aligned with the input sequence):

```python
results = await pipeline.map([1, 2, 3], concurrency=5)
```

Implementation notes:

- Uses **`asyncio.TaskGroup`** to run one async worker per item (not `gather`).
- Uses **`asyncio.Semaphore`** so at most `concurrency` pipelines run at once; workers still start as tasks, but only `concurrency` of them proceed past the semaphore at a time.
- Each worker calls **`run()`** for its item and writes into a pre-sized list by **index**, so results stay in input order even when tasks finish out of order.

**Errors (default):** if any item fails, `TaskGroup` surfaces an **`ExceptionGroup`** (and cancels the other workers). `StageExecutionError` from a stage is propagated like in `run()` (wrapped inside the group as needed).

**Errors (`return_exceptions=True`):** failures are stored in the result list in the matching position as the exception object; the `TaskGroup` completes without raising, so you get `list` entries that are either normal outputs or an `Exception` (often `StageExecutionError`).

## Development commands

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy src
```

## Roadmap

- **Hooks** — before/after each stage or for the full pipeline

## License

See the `LICENSE` file.
