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
    /// Override to provide a shorter, human-readable label — useful for error messages,
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
