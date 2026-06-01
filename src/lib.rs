//! A high-performance, zero-allocation pipeline execution library for Rust.
//!
//! Stages operate on a single pre-allocated [`Scratchpad`][scratchpad::Scratchpad] buffer,
//! reading inputs and writing outputs in place. No heap allocation occurs on the
//! execution hot path.
//!
//! # Pipeline variants
//!
//! Two execution models are provided:
//!
//! - [`dynamic_pipeline::Pipeline`] — stages stored as [`Box<dyn Stage<S>>`][stage::Stage],
//!   supporting heterogeneous stage types configured at runtime.
//! - [`static_pipeline::Pipeline`] — stages stored as a fixed-size array of function
//!   pointers, with no heap allocation after initialisation and no vtable overhead.
//!
//! # Composable wrappers
//!
//! Stages can be decorated with the following wrappers, which compose cleanly:
//!
//! - [`retry::Retry`] — retries a failing stage up to N times, resetting the
//!   scratchpad between attempts.
//! - [`metrics::Timed`] — records per-stage execution latency using lock-free
//!   atomics and a rolling window for percentile computation.
//! - [`instrument::Instrumented`] — emits a [`tracing`] span on every stage execution.
//!
//! # Example
//!
//! ```rust
//! use pipex::scratchpad::Scratchpad;
//! use pipex::stage::Stage;
//! use pipex::dynamic_pipeline::Pipeline;
//! use pipex::error::PipelineError;
//!
//! struct Buffer {
//!     values: Vec<f32>,
//!     output: Vec<f32>,
//! }
//!
//! impl Scratchpad for Buffer {
//!     fn reset(&mut self) { self.output.iter_mut().for_each(|x| *x = 0.0); }
//!     fn validate(&self) -> bool { !self.values.is_empty() }
//! }
//!
//! struct Normalise;
//!
//! impl Stage<Buffer> for Normalise {
//!     fn run(&mut self, ctx: &mut Buffer) -> Result<(), PipelineError> {
//!         let max = ctx.values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
//!         ctx.output.iter_mut().zip(ctx.values.iter()).for_each(|(o, v)| *o = v / max);
//!         Ok(())
//!     }
//! }
//!
//! let mut pipeline = Pipeline::new(Buffer { values: vec![1.0, 2.0, 4.0], output: vec![0.0; 3] })
//!     .stage(Normalise);
//!
//! pipeline.run().unwrap();
//! ```

pub mod deadline;
pub mod dynamic_pipeline;
pub mod error;
pub mod instrument;
pub mod metrics;
pub mod retry;
pub mod scratchpad;
pub mod stage;
pub mod static_pipeline;
