"""Lightweight async pipeline composition."""

from async_pipeline.errors import PipelineError, StageExecutionError
from async_pipeline.pipeline import Pipeline
from async_pipeline.stage import Stage
from async_pipeline.types import AfterStageHook, BeforeStageHook

__all__ = [
    "AfterStageHook",
    "BeforeStageHook",
    "Pipeline",
    "PipelineError",
    "Stage",
    "StageExecutionError",
]

__version__ = "0.6.0"
