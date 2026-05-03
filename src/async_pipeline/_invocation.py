"""Internal helpers for optional execution-context arguments.

Stages and hooks may opt into receiving the execution-context dict as an
extra positional argument. We probe the callable's signature **once** at
construction time (in ``Stage.__init__`` / ``Pipeline.__init__``) and cache
the boolean decision, so the runtime hot path never calls ``inspect`` again.
"""

from collections.abc import Callable
from inspect import Parameter, signature
from typing import Any

_POSITIONAL = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)


def accepts_arity(func: Callable[..., Any], arity: int) -> bool:
    """Return True if ``func`` can be called with ``arity`` positional arguments.

    Counts only ``POSITIONAL_ONLY`` / ``POSITIONAL_OR_KEYWORD`` parameters.
    A ``*args`` parameter makes the callable accept any arity.
    Callables without an introspectable signature (e.g. some C builtins) are
    treated as not supporting the requested arity.
    """
    try:
        params = signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    positional = 0
    for param in params:
        if param.kind is Parameter.VAR_POSITIONAL:
            return True
        if param.kind in _POSITIONAL:
            positional += 1
    return positional >= arity
