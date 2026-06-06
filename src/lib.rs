//! pipex is a zero-allocation stage executor for deterministic workloads. Stages
//! transform a shared scratchpad buffer in sequence; data lives in the scratchpad,
//! the pipeline is reusable logic, and the two meet only at `pipeline.run(&mut ctx)`.
//! Every stage can be timed, traced, and named with no changes to the pipeline or
//! scratchpad.
//!
//! # Design
//!
//! Seven principles guide every decision in this library:
//!
//! 1. **Scratchpad is the execution state.** All inter-stage data lives in the
//!    scratchpad. Stages read from and write to it in place. Nothing is allocated,
//!    copied, or moved between stages.
//!
//! 2. **Pipeline and scratchpad are decoupled.** `pipeline.run(&mut ctx)` borrows
//!    the scratchpad for the duration of execution and returns it. The pipeline
//!    never owns data. Either can be created, pooled, or discarded independently.
//!
//! 3. **The execution path allocates nothing.** Neither pipeline variant calls the
//!    allocator during `run()`, and neither do [`metrics::Timed`],
//!    [`instrument::Instrumented`], or [`deadline::Deadline`]. This is a hard
//!    guarantee verified by the test suite. [`retry::Retry`] is the sole
//!    exception: it clones the scratchpad before each attempt to restore state on
//!    failure, which allocates if the scratchpad contains heap data. `Retry` is an
//!    error-recovery wrapper and should not appear on a zero-allocation hot path.
//!
//! 4. **Execution is deterministic and linear.** Stages run in the order they were
//!    added, on the thread that calls `run()`. No parallelism, no branching, no
//!    scheduler. Given the same scratchpad, the same pipeline always produces the
//!    same result.
//!
//! 5. **Wrappers are independent decorators.** [`retry::Retry`],
//!    [`metrics::Timed`], [`instrument::Instrumented`], and [`deadline::Deadline`]
//!    are each independent [`Stage<S>`][stage::Stage] decorators. None knows about
//!    the others. They compose in any order without hidden coupling.
//!
//! 6. **Observability is first-class.** Named stages, lock-free per-stage timing,
//!    rolling window percentiles, and tracing spans are core library types, not
//!    optional plugins. Every stage can be observed.
//!
//! 7. **No macros required.** [`Scratchpad`][scratchpad::Scratchpad] and
//!    [`Stage`][stage::Stage] are plain `impl` blocks. No derive macros or
//!    attribute macros are needed to use the library.
//!
//! **This library intentionally does not provide:** async stage execution, DAG
//! execution, workflow orchestration, distributed scheduling, or runtime task
//! graphs. These are permanent non-goals, not deferred features.
//!
//! # Pipeline variants
//!
//! - [`static_pipeline::Pipeline`] : stages as function pointers in a fixed array.
//!   Zero allocation after setup. Takes `&self` on `run`, so a single instance can
//!   be shared across threads via `Arc`. The primary recommendation for hot-path
//!   workloads.
//! - [`dynamic_pipeline::Pipeline`] : stages as `Box<dyn Stage<S>>`, configured at
//!   runtime. Use for plugin systems, test harnesses, and configurable pipelines.
//! - **Tuple chain** ([`chain`]) : a tuple of stages is itself a [`Stage<S>`][stage::Stage].
//!   All stage state is stored inline with no heap allocation and no capacity constant.
//!   Wrappers compose naturally as tuple elements:
//!   `let (clamp, clamp_metrics) = Timed::new(Clamp);` then `(Normalise, Retry::new(clamp, 3))`.
//!
//! All three borrow the scratchpad at run time: `pipeline.run(&mut ctx)`.
//!
//! # Composable wrappers
//!
//! - [`retry::Retry`] : retries a failing stage up to N times. Before each
//!   attempt the scratchpad is snapshotted via [`Clone`] and restored on failure,
//!   so only the retried stage's writes are undone. Allocates if the scratchpad
//!   contains heap data; not suitable for zero-allocation hot paths.
//! - [`metrics::Timed`] : records per-stage execution latency using lock-free
//!   atomics and a rolling window for percentile computation.
//! - [`instrument::Instrumented`] : emits a [`tracing`] span on every execution.
//! - [`deadline::Deadline`] : returns an error if a stage exceeds its time budget.
//!
//! # Pooling
//!
//! [`pool::ScratchpadPool`] manages a stock of pre-allocated scratchpads for
//! concurrent workloads. Share one pipeline via `Arc`; each thread acquires a
//! [`pool::ScratchpadGuard`] from the pool, uses it, and on drop the scratchpad
//! is reset and returned.
//!
//! # Example
//!
//! ```rust
//! use pipex::scratchpad::Scratchpad;
//! use pipex::stage::Stage;
//! use pipex::error::PipelineError;
//!
//! struct Buffer {
//!     values: Vec<f32>,
//!     output: Vec<f32>,
//! }
//!
//! impl Scratchpad for Buffer {
//!     fn reset(&mut self) { self.output.iter_mut().for_each(|x| *x = 0.0); }
//! }
//!
//! struct Normalise;
//! struct Clamp;
//!
//! impl Stage<Buffer> for Normalise {
//!     fn run(&mut self, ctx: &mut Buffer) -> Result<(), PipelineError> {
//!         let max = ctx.values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
//!         ctx.output.iter_mut().zip(ctx.values.iter()).for_each(|(o, v)| *o = v / max);
//!         Ok(())
//!     }
//! }
//!
//! impl Stage<Buffer> for Clamp {
//!     fn run(&mut self, ctx: &mut Buffer) -> Result<(), PipelineError> {
//!         ctx.output.iter_mut().for_each(|x| *x = x.clamp(0.0, 1.0));
//!         Ok(())
//!     }
//! }
//!
//! let mut pipeline = (Normalise, Clamp);
//! let mut ctx = Buffer { values: vec![1.0, 2.0, 4.0], output: vec![0.0; 3] };
//! pipeline.run(&mut ctx).unwrap();
//! ```

pub mod chain;
pub mod deadline;
pub mod dynamic_pipeline;
pub mod error;
pub mod instrument;
pub mod metrics;
pub mod pool;
pub mod retry;
pub mod scratchpad;
pub mod stage;
pub mod static_pipeline;
