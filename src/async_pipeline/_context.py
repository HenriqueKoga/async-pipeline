"""Internal helpers for context objects (dict-like or attribute-based)."""

from collections.abc import MutableMapping
from typing import Any


def get_context_value(context: object, key: str, default: Any = None) -> Any:
    """Get a context entry from mapping key or object attribute."""
    if isinstance(context, MutableMapping):
        return context.get(key, default)
    return getattr(context, key, default)


def set_context_value(context: object, key: str, value: Any) -> bool:
    """Set a context entry via mapping key or object attribute.

    Returns ``True`` when the value was persisted on the context object.
    """
    if isinstance(context, MutableMapping):
        context[key] = value
        return True
    try:
        setattr(context, key, value)
    except Exception:
        return False
    return True
