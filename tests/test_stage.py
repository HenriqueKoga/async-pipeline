"""Tests for Stage."""

import pytest

from async_pipeline import Stage, StageExecutionError


async def test_stage_async_handler() -> None:
    async def double(x: int) -> int:
        return x * 2

    stage = Stage("double", double)
    assert await stage.run(3) == 6


async def test_stage_sync_handler() -> None:
    def inc(x: int) -> int:
        return x + 1

    stage = Stage("inc", inc)
    assert await stage.run(1) == 2


async def test_stage_preserves_stage_name_on_error() -> None:
    async def boom(_: int) -> int:
        raise RuntimeError("nope")

    stage = Stage("named_stage", boom)
    with pytest.raises(StageExecutionError) as exc:
        await stage.run(0)
    assert exc.value.stage_name == "named_stage"


async def test_stage_preserves_original_exception() -> None:
    class CustomError(Exception):
        pass

    async def raises_custom(_: int) -> int:
        raise CustomError("inner")

    stage = Stage("s", raises_custom)
    with pytest.raises(StageExecutionError) as exc:
        await stage.run(0)
    assert isinstance(exc.value.original_error, CustomError)


async def test_stage_sync_raise_wrapped() -> None:
    def sync_boom(_: int) -> int:
        raise ValueError("sync")

    stage = Stage("sync_stage", sync_boom)
    with pytest.raises(StageExecutionError) as exc:
        await stage.run(0)
    assert isinstance(exc.value.original_error, ValueError)
