"""Lightweight async pipeline composition."""

from async_pipeline.errors import PipelineError, StageExecutionError
from async_pipeline.pipeline import Pipeline
from async_pipeline.stage import BackoffMode, Stage
from async_pipeline.types import AfterStageHook, BeforeStageHook, Middleware

__all__ = [
    "AfterStageHook",
    "BackoffMode",
    "BeforeStageHook",
    "Middleware",
    "Pipeline",
    "PipelineError",
    "Stage",
    "StageExecutionError",
]

__version__ = "0.8.0"
