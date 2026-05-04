"""Lightweight async pipeline composition."""

from async_pipeline.errors import PipelineError, StageExecutionError
from async_pipeline.pipeline import Pipeline
from async_pipeline.stage import BackoffMode, Stage
from async_pipeline.types import AfterStageHook, BeforeStageHook

__all__ = [
    "AfterStageHook",
    "BackoffMode",
    "BeforeStageHook",
    "Pipeline",
    "PipelineError",
    "Stage",
    "StageExecutionError",
]

__version__ = "0.6.0"
