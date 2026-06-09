//! The [`Scratchpad`] trait: shared mutable state for a pipeline run.
//!
//! A scratchpad is a pre-allocated buffer that all stages read from and write
//! to in sequence. Allocate it once and reuse it across runs.
//! [`pool::ScratchpadPool`][crate::pool::ScratchpadPool] manages a stock of
//! scratchpads for concurrent workloads.

/// A marker trait for types that can be used as a scratchpad in a pipeline.
///
/// Implement this on your own struct to use it with `pipexec`.
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
/// impl pipexec::scratchpad::Scratchpad for MyScratchpad {
///     fn reset(&mut self) {
///         self.values.clear();
///     }
/// }
/// ```
pub trait Scratchpad {
    /// Resets the scratchpad to its initial state, ready for reuse.
    ///
    /// Called by [`ScratchpadPool`][crate::pool::ScratchpadPool] when a
    /// scratchpad is returned to the pool. Not called by the pipeline itself.
    fn reset(&mut self);
}
