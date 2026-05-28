//! Stage trait for defining pipeline computation steps.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;

/// A single computation step in a pipeline.
///
/// Each stage receives a mutable reference to the scratchpad,
/// reads what it needs, and writes its output back in place.
///
/// # Example
/// ```
/// use pipex::stage::Stage;
/// use pipex::scratchpad::Scratchpad;
/// use pipex::error::PipelineError;
///
/// struct DoubleValues;
///
/// impl<S: Scratchpad> Stage<S> for DoubleValues {
///     fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
///         Ok(())
///     }
/// }
/// ```
pub trait Stage<S: Scratchpad> {
    /// Executes this stage against the provided scratchpad.
    ///
    /// Returns `Ok(())` on success, or a `PipelineError` on failure.
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError>;
}

// Provided for downstream wiring code that builds stages incrementally at
// runtime — allows a boxed stage to be passed into Timed, Instrumented, or
// Retry without knowing the concrete type at compile time.
impl<S: Scratchpad> Stage<S> for Box<dyn Stage<S>> {
    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        (**self).run(ctx)
    }
}
