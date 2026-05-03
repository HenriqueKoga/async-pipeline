"""Errors raised by async-pipeline."""


class PipelineError(Exception):
    """Base exception for pipeline-related failures."""


class StageExecutionError(PipelineError):
    """Raised when a stage handler fails."""

    def __init__(self, stage_name: str, original_error: Exception) -> None:
        self.stage_name = stage_name
        self.original_error = original_error
        super().__init__(f"Stage {stage_name!r} failed: {original_error!r}")
