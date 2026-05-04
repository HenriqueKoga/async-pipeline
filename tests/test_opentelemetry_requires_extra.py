"""Import error when OpenTelemetry is not installed (simulated)."""

import builtins
import importlib
import sys

import pytest


def test_opentelemetry_submodule_raises_clear_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``opentelemetry.py`` must fail fast with the documented message."""
    mod = "async_pipeline.telemetry.opentelemetry"
    sys.modules.pop(mod, None)
    for name in list(sys.modules):
        if name.startswith("opentelemetry"):
            del sys.modules[name]

    real_import = builtins.__import__

    def fake_import(
        name: str,
        globals_arg: dict[str, object] | None = None,
        locals_arg: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            raise ModuleNotFoundError("No module named 'opentelemetry'")
        return real_import(name, globals_arg, locals_arg, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    try:
        with pytest.raises(ImportError, match=r"async-pipeline\[otel\]"):
            importlib.import_module(mod)
    finally:
        monkeypatch.setattr(builtins, "__import__", real_import)
        importlib.invalidate_caches()
        importlib.import_module("opentelemetry.trace")
        sys.modules.pop("async_pipeline.telemetry.opentelemetry", None)
        importlib.import_module("async_pipeline.telemetry.opentelemetry")


def test_telemetry_getattr_raises_without_otel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lazy ``OpenTelemetryMiddleware`` export surfaces the same guidance."""
    import async_pipeline.telemetry as tel_pkg

    def boom(_name: str) -> object:
        raise ModuleNotFoundError("opentelemetry")

    monkeypatch.setattr(tel_pkg.importlib, "import_module", boom)
    try:
        with pytest.raises(ImportError, match=r"async-pipeline\[otel\]"):
            getattr(tel_pkg, "OpenTelemetryMiddleware")
    finally:
        monkeypatch.undo()
