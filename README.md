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
- **Concurrent map** — a stage that processes collections with bounded concurrency

## License

See the `LICENSE` file.
