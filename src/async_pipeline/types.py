"""Public type aliases for async-pipeline."""

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

ContextT = TypeVar("ContextT", bound=object)
type DefaultContext = dict[str, Any]

Middleware = Callable[
    [Callable[[Any], Awaitable[Any]], str, Any, object],
    Any,
]

BeforeStageHook = (
    Callable[[str, Any], None | Awaitable[None]]
    | Callable[[str, Any, object], None | Awaitable[None]]
)
AfterStageHook = (
    Callable[[str, Any, Any | None, Exception | None], None | Awaitable[None]]
    | Callable[
        [str, Any, Any | None, Exception | None, object],
        None | Awaitable[None],
    ]
)
