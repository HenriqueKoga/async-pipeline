"""Tests for Stage."""

import asyncio

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


async def test_stage_async_with_timeout_sufficient() -> None:
    async def slow(value: int) -> int:
        await asyncio.sleep(0.01)
        return value

    stage = Stage("slow", slow, timeout=1.0)
    result = await stage.run(10)
    assert result == 10


async def test_stage_async_timeout_exceeded() -> None:
    async def slow(value: int) -> int:
        await asyncio.sleep(0.1)
        return value

    stage = Stage("slow", slow, timeout=0.01)
    with pytest.raises(StageExecutionError) as exc:
        await stage.run(10)
    assert exc.value.stage_name == "slow"
    assert isinstance(exc.value.original_error, TimeoutError)


async def test_stage_sync_with_timeout_ignored() -> None:
    def add_one(value: int) -> int:
        return value + 1

    stage = Stage("add_one", add_one, timeout=1.0)
    result = await stage.run(1)
    assert result == 2


def test_stage_invalid_timeout_zero() -> None:
    def ident(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Stage("invalid", ident, timeout=0)


def test_stage_invalid_timeout_negative() -> None:
    def ident(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="timeout must be greater than 0"):
        Stage("invalid", ident, timeout=-1.0)
