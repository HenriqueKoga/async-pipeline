"""Logging middleware around each stage."""

import logging
from collections.abc import Awaitable, Callable
from typing import Any


class LoggingMiddleware:
    """Log each stage start, success, and failure via :mod:`logging`.

    Exceptions from ``next`` are logged and re-raised unchanged. Works with
    :class:`~async_pipeline.Pipeline` hooks, context, and other middlewares.
    """

    def __init__(
        self,
        logger: logging.Logger | None = None,
        level: int = logging.INFO,
        *,
        include_value: bool = False,
    ) -> None:
        """Configure logger, level, and whether payloads appear in messages.

        Args:
            logger: Target logger; defaults to ``logging.getLogger("async_pipeline")``.
            level: Log level for start/finish lines (errors use ``exception``).
            include_value: When ``True``, include repr of input/output in logs.
        """
        self._logger = logger or logging.getLogger("async_pipeline")
        self._level = level
        self._include_value = include_value

    async def __call__(
        self,
        next_fn: Callable[[Any], Awaitable[Any]],
        stage_name: str,
        value: Any,
        _context: dict[str, Any],
    ) -> Any:
        if self._include_value:
            self._logger.log(
                self._level,
                "Starting stage: %s input=%r",
                stage_name,
                value,
            )
        else:
            self._logger.log(self._level, "Starting stage: %s", stage_name)
        try:
            result = await next_fn(value)
        except Exception:
            if self._include_value:
                self._logger.exception(
                    "Stage failed: %s input=%r",
                    stage_name,
                    value,
                )
            else:
                self._logger.exception("Stage failed: %s", stage_name)
            raise
        if self._include_value:
            self._logger.log(
                self._level,
                "Finished stage: %s output=%r",
                stage_name,
                result,
            )
        else:
            self._logger.log(self._level, "Finished stage: %s", stage_name)
        return result
