"""Internal helpers for optional execution-context arguments and hook adapters.

Stages may receive ``(value)`` or ``(value, context)``. Hooks may use legacy
arity or include ``context`` as the last positional argument. Signature checks
run **once** when building ``Stage`` / ``Pipeline``; normalized pipeline hooks
are always async callables with a fixed arity so ``run()`` stays branch-free.
"""

from collections.abc import Awaitable, Callable
from inspect import Parameter, isawaitable, signature
from typing import Any

_POSITIONAL = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)

# Normalized hook runners (always async; swallow errors at call site in Pipeline).
BeforeHookRunner = Callable[[str, Any, dict[str, Any]], Awaitable[None]]
AfterHookRunner = Callable[
    [str, Any, Any | None, Exception | None, dict[str, Any]],
    Awaitable[None],
]


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


def normalize_before_hook(hook: Callable[..., Any]) -> BeforeHookRunner:
    """Wrap a sync/async before hook so it always takes ``(name, input, context)``."""
    wants = accepts_arity(hook, 3)

    async def wrapped(name: str, inp: Any, ctx: dict[str, Any]) -> None:
        result = hook(name, inp, ctx) if wants else hook(name, inp)
        if isawaitable(result):
            await result

    return wrapped


def normalize_after_hook(hook: Callable[..., Any]) -> AfterHookRunner:
    """Wrap after hook to fixed arity: name, input, output, error, context."""
    wants = accepts_arity(hook, 5)

    async def wrapped(
        name: str,
        inp: Any,
        out: Any | None,
        err: Exception | None,
        ctx: dict[str, Any],
    ) -> None:
        result = hook(name, inp, out, err, ctx) if wants else hook(name, inp, out, err)
        if isawaitable(result):
            await result

    return wrapped
