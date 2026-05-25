//! Scratchpad trait for defining reusable pipeline buffers.

/// A marker trait for types that can be used as a scratchpad in a pipeline.
///
/// Implement this on your own struct to use it with `pipex`.
///
/// # Example
/// ```
/// struct MyScratchpad {
///     values: Vec<f32>,
/// }
///
/// impl pipex::scratchpad::Scratchpad for MyScratchpad {
///     fn reset(&mut self) {
///         self.values.clear();
///     }
///
///     fn validate(&self) -> bool {
///         !self.values.is_empty()
///     }
/// }
/// ```
pub trait Scratchpad {
    /// Resets the scratchpad to its initial state, ready for reuse.
    ///
    /// Called by the pipeline between runs.
    fn reset(&mut self);

    /// Validates that the scratchpad is in a usable state before
    /// the pipeline begins execution.
    ///
    /// Returns `true` if valid, `false` otherwise.
    fn validate(&self) -> bool;
}
