"""Public type aliases for async-pipeline."""

from collections.abc import Awaitable, Callable
from typing import Any

BeforeStageHook = (
    Callable[[str, Any], None | Awaitable[None]]
    | Callable[[str, Any, dict[str, Any]], None | Awaitable[None]]
)
AfterStageHook = (
    Callable[[str, Any, Any | None, Exception | None], None | Awaitable[None]]
    | Callable[
        [str, Any, Any | None, Exception | None, dict[str, Any]],
        None | Awaitable[None],
    ]
)
