# Contributing

Thank you for helping improve **async-pipeline**.

## Local setup

Install [uv](https://docs.astral.sh/uv/), clone the repository, then from the project root:

```bash
uv sync --extra otel
```

The OpenTelemetry extra matches CI and is required to import `OpenTelemetryMiddleware` in tests and optional examples.

## Checks before opening a PR

```bash
uv run pytest
uv run ruff check .
uv run mypy src
uv build
uv run twine check dist/*
```

## Executable examples

See [`examples/README.md`](examples/README.md). Smoke tests run the scripts via `tests/test_examples.py`.

## Branches

- Prefer feature branches off `master`, e.g. `feat/…`, `fix/…`, `chore/…`.
- Keep PRs focused and small when possible.

## Commits

- Use clear, imperative subject lines (e.g. `fix: handle empty map input`).
- Conventional prefixes (`feat:`, `fix:`, `docs:`, `chore:`) are welcome but not enforced by tooling in this repo.

## Pull request flow

1. Fork / branch from `master`.
2. Run the checks above.
3. Open a PR with a short description of behavior changes and risk.
4. CI must be green before merge.

## Release process (maintainers)

Releases are automated on **push to `master`** (see [`.github/workflows/release.yml`](.github/workflows/release.yml)):

1. Bump **`version`** in `pyproject.toml` (and `src/async_pipeline/__init__.py` `__version__` to match).
2. Update **`CHANGELOG.md`**.
3. Merge to `master`.

The workflow reads the version from `pyproject.toml`, builds, runs `twine check`, publishes to **PyPI** using **Trusted Publishing** (OIDC), then creates the tag `v<version>` **only if that tag does not already exist**—so the same version is never published twice from this automation.

Configure the PyPI project to trust this GitHub repository and workflow per [PyPI’s trusted publishing guide](https://docs.pypi.org/trusted-publishers/).

## Versioning policy

This project follows [Semantic Versioning](https://semver.org/). Public symbols exported from `async_pipeline`, `async_pipeline.middlewares`, and `async_pipeline.telemetry` are treated as stable API for 1.x unless documented otherwise in the changelog.
