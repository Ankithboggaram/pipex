//! Static pipeline executor using function pointers for zero heap allocation.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;

type StageFn<S> = fn(&mut S) -> Result<(), PipelineError>;

/// A fixed-capacity pipeline that stores stages as function pointers.
///
/// Unlike [`DynamicPipeline`][crate::dynamic_pipeline::Pipeline], no heap
/// allocation occurs after initialisation. The number of stages `N` must be
/// known at compile time.
///
/// The pipeline holds no data; stages are run by passing a mutable scratchpad
/// reference to [`run`][Pipeline::run]. Because the pipeline has no mutable
/// state after setup, `run` takes `&self`, allowing a single pipeline to be
/// shared across threads via [`Arc`] while each thread supplies its own
/// scratchpad.
///
/// Aligned to 64 bytes (one cache line) so the function pointer array is
/// read from a clean cache-line boundary on every `run()` call.
///
/// # Example
/// ```
/// use pipex::static_pipeline::Pipeline;
/// use pipex::scratchpad::Scratchpad;
/// use pipex::error::PipelineError;
///
/// struct Buf { value: f32 }
///
/// impl Scratchpad for Buf {
///     fn reset(&mut self) { self.value = 0.0; }
/// }
///
/// fn double(ctx: &mut Buf) -> Result<(), PipelineError> {
///     ctx.value *= 2.0;
///     Ok(())
/// }
///
/// let mut pipeline = Pipeline::<Buf, 1>::new();
/// pipeline.add_stage(double).unwrap();
///
/// let mut ctx = Buf { value: 2.0 };
/// pipeline.run(&mut ctx).unwrap();
/// assert_eq!(ctx.value, 4.0);
/// ```
#[repr(align(64))]
pub struct Pipeline<S: Scratchpad, const N: usize> {
    stages: [Option<StageFn<S>>; N],
    count: usize,
}

impl<S: Scratchpad, const N: usize> Default for Pipeline<S, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Scratchpad, const N: usize> std::fmt::Debug for Pipeline<S, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("stages", &self.count)
            .field("capacity", &N)
            .finish()
    }
}

impl<S: Scratchpad, const N: usize> Pipeline<S, N> {
    /// Creates a new empty static pipeline with capacity for `N` stages.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stages: [None; N],
            count: 0,
        }
    }

    /// Adds a stage function to the pipeline.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::FullPipeline` if the pipeline is already at capacity.
    pub fn add_stage(&mut self, stage: StageFn<S>) -> Result<(), PipelineError> {
        if self.count >= N {
            return Err(PipelineError::FullPipeline);
        }
        self.stages[self.count] = Some(stage);
        self.count += 1;
        Ok(())
    }

    /// Returns `true` if the pipeline contains the given stage function.
    #[must_use]
    pub fn contains_stage_fn(&self, stage: StageFn<S>) -> bool {
        self.stages[..self.count]
            .iter()
            .any(|s| s.is_some_and(|f| std::ptr::fn_addr_eq(f, stage)))
    }

    /// Returns the index of the first occurrence of the given stage function,
    /// or `None` if not present.
    #[must_use]
    pub fn stage_fn_position(&self, stage: StageFn<S>) -> Option<usize> {
        self.stages[..self.count]
            .iter()
            .position(|s| s.is_some_and(|f| std::ptr::fn_addr_eq(f, stage)))
    }

    /// Validates the pipeline's stage configuration with a user-provided closure.
    ///
    /// Receives `&self.stages[..count]` — a slice of `Option<StageFn<S>>` with no
    /// intermediate allocation. Every entry in the slice is `Some`; the `Option`
    /// wrapper is a consequence of the fixed-size backing array.
    ///
    /// # Errors
    ///
    /// Returns the error returned by `validator`, if any.
    pub fn check<F>(&self, validator: F) -> Result<(), PipelineError>
    where
        F: FnOnce(&[Option<StageFn<S>>]) -> Result<(), PipelineError>,
    {
        validator(&self.stages[..self.count])
    }

    /// Runs all stages in order against the provided scratchpad.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::EmptyPipeline` if no stages have been added,
    /// or the error from the first stage that fails.
    #[inline]
    pub fn run(&self, ctx: &mut S) -> Result<(), PipelineError> {
        if self.count == 0 {
            return Err(empty_pipeline());
        }

        for i in 0..self.count {
            if let Some(stage) = self.stages[i] {
                stage(ctx)?;
            }
        }

        Ok(())
    }
}

#[cold]
#[inline(never)]
fn empty_pipeline() -> PipelineError {
    PipelineError::EmptyPipeline
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestScratchpad {
        value: f32,
    }

    impl TestScratchpad {
        fn new(value: f32) -> Self {
            Self { value }
        }
    }

    impl Scratchpad for TestScratchpad {
        fn reset(&mut self) {
            self.value = 0.0;
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn double(ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
        ctx.value *= 2.0;
        Ok(())
    }

    #[allow(clippy::unnecessary_wraps)]
    fn failing(_ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
        Err(PipelineError::StageFailed(String::from(
            "intentional failure",
        )))
    }

    #[test]
    fn empty_pipeline_returns_error() {
        let pipeline = Pipeline::<TestScratchpad, 2>::new();
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::EmptyPipeline)
        ));
    }

    #[test]
    fn stage_runs_and_modifies_scratchpad() {
        let mut pipeline = Pipeline::<TestScratchpad, 1>::new();
        pipeline.add_stage(double).unwrap();
        let mut ctx = TestScratchpad::new(2.0);
        assert!(pipeline.run(&mut ctx).is_ok());
        assert_eq!(ctx.value, 4.0);
    }

    #[test]
    fn pipeline_at_capacity_returns_error() {
        let mut pipeline = Pipeline::<TestScratchpad, 1>::new();
        pipeline.add_stage(double).unwrap();
        assert!(matches!(
            pipeline.add_stage(failing),
            Err(PipelineError::FullPipeline)
        ));
    }

    #[test]
    fn failing_stage_returns_error() {
        let mut pipeline = Pipeline::<TestScratchpad, 1>::new();
        pipeline.add_stage(failing).unwrap();
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn contains_stage_fn_detects_added_function() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new();
        pipeline.add_stage(double).unwrap();
        assert!(pipeline.contains_stage_fn(double));
        assert!(!pipeline.contains_stage_fn(failing));
    }

    #[test]
    fn stage_fn_position_returns_correct_index() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new();
        pipeline.add_stage(double).unwrap();
        pipeline.add_stage(failing).unwrap();
        assert_eq!(pipeline.stage_fn_position(double), Some(0));
        assert_eq!(pipeline.stage_fn_position(failing), Some(1));
    }

    #[test]
    fn check_validates_stage_ordering() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new();
        pipeline.add_stage(double).unwrap();
        pipeline.add_stage(failing).unwrap();
        assert!(
            pipeline
                .check(|fns| {
                    let double_pos = fns.iter().position(|f| {
                        f.is_some_and(|f| {
                            std::ptr::fn_addr_eq(f, double as StageFn<TestScratchpad>)
                        })
                    });
                    let fail_pos = fns.iter().position(|f| {
                        f.is_some_and(|f| {
                            std::ptr::fn_addr_eq(f, failing as StageFn<TestScratchpad>)
                        })
                    });
                    match (double_pos, fail_pos) {
                        (Some(d), Some(f)) if d < f => Ok(()),
                        _ => Err(PipelineError::InvalidState("wrong order".into())),
                    }
                })
                .is_ok()
        );
    }

    #[test]
    fn pipeline_can_be_shared_across_threads() {
        use std::sync::Arc;

        let mut pipeline = Pipeline::<TestScratchpad, 1>::new();
        pipeline.add_stage(double).unwrap();
        let pipeline = Arc::new(pipeline);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let pipeline = Arc::clone(&pipeline);
                std::thread::spawn(move || {
                    let mut ctx = TestScratchpad::new(2.0);
                    pipeline.run(&mut ctx).unwrap();
                    assert_eq!(ctx.value, 4.0);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
