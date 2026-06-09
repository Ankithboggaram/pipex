# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2026-06-08

### Fixed
- Corrected crates.io categories: replaced `embedded` (implies `no_std`, which this
  crate does not support) and `concurrency` with `algorithms` and `science::ml`.

## [0.3.0] - 2026-06-08

### Added
- `ScratchpadPool::acquire_owned` — returns an `OwnedScratchpadGuard` that holds
  an `Arc` clone of the pool instead of a lifetime-bound reference. Callers in
  async contexts (e.g. tonic gRPC handlers) can hold the guard across `.await`
  points without lifetime conflicts.
- `OwnedScratchpadGuard` — public type returned by `acquire_owned`. Identical
  semantics to `ScratchpadGuard`: resets and returns the scratchpad on drop.

### Changed
- **BREAKING**: `PipelineError::StageFailed` field `message: String` renamed to
  `source: Box<dyn std::error::Error + Send + Sync>`. Callers constructing this
  variant must update to `source: "message text".into()` or
  `source: Box::new(original_error)`. The underlying error is now also exposed
  via `std::error::Error::source`, enabling proper error chain inspection.

## [0.2.0] - 2025-05-01

### Added
- `ScratchpadPool` and `ScratchpadGuard` for pooling pre-allocated scratchpads
  across concurrent pipeline workloads.
- `Deadline` stage wrapper — fails with `PipelineError::DeadlineExceeded` if the
  inner stage exceeds a `Duration` budget.
- `Instrumented` stage wrapper — emits tracing spans around each stage execution.
- `PipelineMetrics` and `Timed` — lock-free per-stage timing with rolling window
  percentiles (p50 / p95 / p99 / p999).
- Tuple-chain `Stage` impls for 1- through 8-element tuples, enabling zero-overhead
  inline pipeline composition without the pipeline container types.
- `Stage::name()` on all built-in types and wrappers.
- `static_pipeline::Pipeline::check()` for validating stage order at build time.
- `deny.toml` for `cargo-deny` license / advisory checking.
- CI workflow with test, clippy, doc, and deny jobs.
- Examples: `static_pipeline`, `dynamic_pipeline`, `tuple_chain`, `pooling`.

### Changed
- `PipelineError` is now `#[non_exhaustive]`.
- `ScratchpadPool::acquire` and `PipelinePool::acquire` are now `#[must_use]`.

## [0.1.0] - 2025-01-01

### Added
- `static_pipeline::Pipeline` — fixed-capacity, function-pointer-based pipeline.
  `Arc`-shareable, zero allocation on the hot path.
- `dynamic_pipeline::Pipeline` — heap-allocated boxed-trait-object pipeline for
  runtime composition.
- `Stage` trait with `run(&mut self, ctx: &mut S)` and `name()`.
- `Scratchpad` trait with `reset(&mut self)`.
- `Retry` stage wrapper with snapshot-and-restore semantics.
- `PipelineError` with `StageFailed`, `EmptyPipeline`, `FullPipeline`,
  `InvalidState`, `RetryExhausted`, and `DeadlineExceeded` variants.

[Unreleased]: https://github.com/Ankithboggaram/pipex/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/Ankithboggaram/pipex/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Ankithboggaram/pipex/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Ankithboggaram/pipex/releases/tag/v0.1.0
