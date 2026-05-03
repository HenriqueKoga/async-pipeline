"""Internal helpers for optional execution-context arguments."""

import inspect
from collections.abc import Callable
from typing import Any


def _param_count(obj: Callable[..., Any]) -> int:
    try:
        return len(inspect.signature(obj).parameters)
    except (TypeError, ValueError):
        return 0


def handler_wants_context(handler: Callable[..., Any]) -> bool:
    """True if handler is defined as (value, context, ...)."""
    return _param_count(handler) >= 2


def before_hook_wants_context(hook: Callable[..., Any]) -> bool:
    """True if hook accepts (stage_name, input_value, context, ...)."""
    return _param_count(hook) >= 3


def after_hook_wants_context(hook: Callable[..., Any]) -> bool:
    """True if hook accepts (..., output, error, context, ...)."""
    return _param_count(hook) >= 5
