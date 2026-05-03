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

- **Retry** — retry policies per stage or for the whole pipeline
- **Timeout** — cap how long a stage may run
- **Hooks** — before/after each stage or the full pipeline

## License

See the `LICENSE` file.
