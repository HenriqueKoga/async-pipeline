# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.0]

### Added

- Optional OpenTelemetry middleware (`async-pipeline[otel]` extra)
- OpenTelemetry is not required by the core package
- `OpenTelemetryMiddleware` creates one span per stage (`{span_prefix}.{stage_name}`)
- Custom span attributes via execution context key `trace_attributes` (simple types only)

## [0.7.0]

### Added

- Middleware support on `Pipeline` (`middlewares=[...]`)
- Per-stage middleware chain (`next`, `stage_name`, `value`, `context`); order is list order outside-in
- Middleware integrates with execution context, hooks, and existing `Stage` retry/timeout behavior

## [0.6.0]

### Added

- Execution context: optional `**context` on `Pipeline.run` and `Pipeline.map`
- Stages may accept `(value)` or `(value, context)`; detection via `inspect.signature`
- Hooks may accept legacy arity or an extra `context` argument (`before_stage`, `after_stage`)
- Per-item shallow copy of the context template in `Pipeline.map` for safe concurrency

### Changed

- None (backward compatible with 0.5.x handlers and hooks)

## [0.5.0]

### Added

- `before_stage` and `after_stage` hooks on `Pipeline`
- Sync and async hooks; hook failures do not interrupt the pipeline or mask stage errors

## [0.4.0]

### Added

- Stage retry with `retries`, `retry_delay`, and `backoff` (`"fixed"` | `"exponential"`)
- Retries apply to `Exception` (including `TimeoutError` from `asyncio.timeout`); no retry on `BaseException` cancellation types

## [0.3.0]

### Added

- Optional per-stage `timeout` using `asyncio.timeout` (awaitable handlers only)

## [0.2.0]

### Added

- `Pipeline.map` for batch runs with bounded concurrency (`asyncio.Semaphore` + `asyncio.TaskGroup`)
- Ordered results; optional `return_exceptions=True`

## [0.1.0]

### Added

- `Pipeline` and `Stage` for sequential async (and sync) composition
- `StageExecutionError` / `PipelineError` and strict-friendly typing
