//! Scratchpad trait for defining reusable pipeline buffers.

/// A marker trait for types that can be used as a scratchpad in a pipeline.
///
/// Implement this on your own struct to use it with `pipex`.
///
/// For best performance on hot paths, consider aligning your scratchpad
/// to a cache line boundary to avoid false sharing:
///
/// ```
/// #[repr(align(64))]
/// struct MyScratchpad {
///     values: Vec<f32>,
/// }
/// ```
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
/// }
/// ```
pub trait Scratchpad {
    /// Resets the scratchpad to its initial state, ready for reuse.
    ///
    /// Called by the pipeline between runs.
    fn reset(&mut self);
}
