"""Smoke-test example scripts (no network, no external services)."""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

EXAMPLE_SCRIPTS = [
    "examples/basic_pipeline.py",
    "examples/batch_processing.py",
    "examples/retry_timeout.py",
    "examples/hooks_context.py",
    "examples/middlewares.py",
    "examples/opentelemetry.py",
    "examples/typed_context.py",
]


@pytest.mark.parametrize("relative_path", EXAMPLE_SCRIPTS)
def test_example_script_runs(relative_path: str) -> None:
    script = REPO_ROOT / relative_path
    completed = subprocess.run(
        [sys.executable, str(script)],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, (
        f"{relative_path} failed:\n{completed.stdout}\n{completed.stderr}"
    )
