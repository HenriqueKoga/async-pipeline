"""Internal helpers: signature introspection and pipeline hook adapters.

Stages may receive ``(value)`` or ``(value, context)``; hooks may use the
legacy arity or include the execution-context object as the last positional
argument. Signature checks happen **once** when ``Stage`` / ``Pipeline`` is
built; the adapters returned by :func:`normalize_before_hook` and
:func:`normalize_after_hook` are always async with a fixed arity, so the hot
path in ``Pipeline.run`` stays branch-free.
"""

from collections.abc import Awaitable, Callable
from inspect import Parameter, isawaitable, signature
from typing import Any

_POSITIONAL_KINDS = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)

BeforeHookRunner = Callable[[str, Any, object], Awaitable[None]]
AfterHookRunner = Callable[
    [str, Any, Any | None, Exception | None, object],
    Awaitable[None],
]


def accepts_arity(func: Callable[..., Any], arity: int) -> bool:
    """Return True if ``func`` can be called with ``arity`` positional arguments.

    Counts only ``POSITIONAL_ONLY`` / ``POSITIONAL_OR_KEYWORD`` parameters.
    A ``*args`` parameter accepts any arity.
    Callables without an introspectable signature (some C builtins) are
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
        if param.kind in _POSITIONAL_KINDS:
            positional += 1
    return positional >= arity


def normalize_before_hook(hook: Callable[..., Any]) -> BeforeHookRunner:
    """Adapt a sync/async before hook to the fixed (name, input, context) shape."""
    wants_context = accepts_arity(hook, 3)

    async def runner(name: str, value: Any, ctx: object) -> None:
        result = hook(name, value, ctx) if wants_context else hook(name, value)
        if isawaitable(result):
            await result

    return runner


def normalize_after_hook(hook: Callable[..., Any]) -> AfterHookRunner:
    """Adapt a sync/async after hook to the fixed arity used by Pipeline."""
    wants_context = accepts_arity(hook, 5)

    async def runner(
        name: str,
        value: Any,
        output: Any | None,
        error: Exception | None,
        ctx: object,
    ) -> None:
        result = (
            hook(name, value, output, error, ctx)
            if wants_context
            else hook(name, value, output, error)
        )
        if isawaitable(result):
            await result

    return runner
