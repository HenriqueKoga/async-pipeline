"""Tests for Pipeline."""

import pytest

from async_pipeline import Pipeline, Stage, StageExecutionError


async def test_pipeline_runs_async_stages_in_order() -> None:
    async def add_one(value: int) -> int:
        return value + 1

    async def multiply_by_two(value: int) -> int:
        return value * 2

    pipeline = Pipeline(
        [
            Stage("add_one", add_one),
            Stage("multiply_by_two", multiply_by_two),
        ]
    )
    result = await pipeline.run(10)
    assert result == 22


async def test_pipeline_supports_sync_stage() -> None:
    def add_one(value: int) -> int:
        return value + 1

    pipeline = Pipeline([Stage("add_one", add_one)])
    result = await pipeline.run(1)
    assert result == 2


async def test_pipeline_raises_stage_execution_error() -> None:
    async def broken(value: int) -> int:
        raise RuntimeError("boom")

    pipeline = Pipeline([Stage("broken", broken)])
    with pytest.raises(StageExecutionError) as exc:
        await pipeline.run(1)
    assert exc.value.stage_name == "broken"
    assert isinstance(exc.value.original_error, RuntimeError)


def test_pipeline_empty_raises_value_error() -> None:
    with pytest.raises(ValueError, match="at least one stage"):
        Pipeline([])
