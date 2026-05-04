"""Errors raised by async-pipeline."""


class PipelineError(Exception):
    """Base class for library errors raised outside normal control flow."""


class StageExecutionError(PipelineError):
    """Raised when :meth:`Stage.run` exhausts retries without success.

    Attributes:
        stage_name: Name of the failing :class:`~async_pipeline.Stage`.
        original_error: Last exception from the handler (e.g. ``RuntimeError``,
            ``TimeoutError``) before wrapping.
    """

    def __init__(self, stage_name: str, original_error: Exception) -> None:
        super().__init__(f"Stage {stage_name!r} failed: {original_error!r}")
        self.stage_name = stage_name
        self.original_error = original_error
