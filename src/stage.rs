//! The [`Stage`] trait: the core building block of every pipeline.
//!
//! Implement `Stage<S>` on any type to make it a pipeline step. Stages are
//! composed into pipelines via tuples ([`chain`][crate::chain]),
//! [`dynamic_pipeline::Pipeline`][crate::dynamic_pipeline::Pipeline], or
//! [`static_pipeline::Pipeline`][crate::static_pipeline::Pipeline].

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;

/// A single computation step in a pipeline.
///
/// Each stage receives a mutable reference to the scratchpad,
/// reads what it needs, and writes its output back in place.
///
/// # Send requirement
///
/// This trait requires [`Send`] because the standard usage model is a single
/// `Arc<Pipeline>` shared across threads backed by a
/// [`ScratchpadPool`][crate::pool::ScratchpadPool]. The pipeline travels
/// between threads, so its stages must be `Send`. Types that contain
/// [`std::rc::Rc`] or [`std::cell::RefCell`] are not `Send` and cannot be
/// used as stages.
///
/// # Example
/// ```
/// use pipexec::stage::Stage;
/// use pipexec::scratchpad::Scratchpad;
/// use pipexec::error::PipelineError;
///
/// struct Buf { value: f32 }
/// impl Scratchpad for Buf { fn reset(&mut self) { self.value = 0.0; } }
///
/// struct Double;
///
/// impl Stage<Buf> for Double {
///     fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
///         ctx.value *= 2.0;
///         Ok(())
///     }
///
///     fn name(&self) -> &'static str { "Double" }
/// }
///
/// let mut stage = Double;
/// let mut ctx = Buf { value: 3.0 };
/// stage.run(&mut ctx).unwrap();
/// assert_eq!(ctx.value, 6.0);
/// ```
pub trait Stage<S: Scratchpad>: Send {
    /// Executes this stage against the provided scratchpad.
    ///
    /// # Errors
    ///
    /// Returns a `PipelineError` if the stage fails during execution.
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError>;

    /// Returns the name of this stage.
    ///
    /// Defaults to the fully qualified type name (e.g. `"mycrate::stages::Normalise"`).
    /// Override to provide a shorter, human-readable label, useful for error messages,
    /// metrics labels, and tracing spans.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

// Provided for wiring code that builds stages incrementally at
// runtime. Allows a boxed stage to be passed into Timed, Instrumented, or
// Retry without knowing the concrete type at compile time.
impl<S: Scratchpad> Stage<S> for Box<dyn Stage<S>> {
    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        (**self).run(ctx)
    }

    fn name(&self) -> &'static str {
        (**self).name()
    }
}
