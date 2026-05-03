"""Pipeline of sequential stages."""

from collections.abc import Sequence
from typing import Any, TypeVar

from async_pipeline.stage import Stage

T = TypeVar("T")


class Pipeline:
    """Runs stages in order, passing each output as the next input."""

    __slots__ = ("_stages",)

    def __init__(self, stages: Sequence[Stage[Any, Any]]) -> None:
        if not stages:
            msg = "Pipeline requires at least one stage"
            raise ValueError(msg)
        self._stages = tuple(stages)

    async def run(self, initial_value: T) -> Any:
        value: Any = initial_value
        for stage in self._stages:
            value = await stage.run(value)
        return value
