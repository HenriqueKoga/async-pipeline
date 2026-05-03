"""Lightweight async pipeline composition."""

from async_pipeline.errors import PipelineError, StageExecutionError
from async_pipeline.pipeline import Pipeline
from async_pipeline.stage import Stage

__all__ = [
    "Pipeline",
    "PipelineError",
    "Stage",
    "StageExecutionError",
]

__version__ = "0.1.0"
