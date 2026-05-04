# Examples

Small, self-contained scripts for **async-pipeline**. Run from the repository root:

```bash
uv run python examples/basic_pipeline.py
```

Or after `uv sync`:

```bash
uv run python examples/batch_processing.py
```

| Script | Topic |
| --- | --- |
| [`basic_pipeline.py`](basic_pipeline.py) | Minimal `Pipeline` + `Stage` |
| [`batch_processing.py`](batch_processing.py) | `Pipeline.map` |
| [`retry_timeout.py`](retry_timeout.py) | `Stage` retries and timeout |
| [`hooks_context.py`](hooks_context.py) | Hooks + execution context |
| [`middlewares.py`](middlewares.py) | Built-in middlewares |
| [`opentelemetry.py`](opentelemetry.py) | Optional tracing (`async-pipeline[otel]`) |

`opentelemetry.py` exits immediately with a short message if the optional dependency is not installed.
