//! Static pipeline executor using function pointers for zero heap allocation.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;

type StageFn<S> = fn(&mut S) -> Result<(), PipelineError>;

/// A fixed-capacity pipeline that stores stages as function pointers.
///
/// Unlike `DynamicPipeline`, no heap allocation occurs after initialisation.
/// The number of stages `N` must be known at compile time.
///
/// The pipeline owns its scratchpad. Use [`context`][Pipeline::context] and
/// [`context_mut`][Pipeline::context_mut] to write inputs and read outputs.
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
/// struct MyScratchpad { value: f32 }
///
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) { self.value = 0.0; }
///     fn validate(&self) -> bool { true }
/// }
///
/// fn double(ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
///     ctx.value *= 2.0;
///     Ok(())
/// }
///
/// let mut pipeline = Pipeline::<MyScratchpad, 1>::new(MyScratchpad { value: 2.0 });
/// pipeline.add_stage(double).unwrap();
/// ```
#[repr(align(64))]
pub struct Pipeline<S: Scratchpad, const N: usize> {
    stages: [Option<StageFn<S>>; N],
    ctx: S,
    count: usize,
    validated: bool,
}

impl<S: Scratchpad, const N: usize> std::fmt::Debug for Pipeline<S, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("stages", &self.count)
            .field("capacity", &N)
            .field("validated", &self.validated)
            .finish()
    }
}

impl<S: Scratchpad, const N: usize> Pipeline<S, N> {
    /// Creates a new empty static pipeline with capacity for `N` stages.
    #[must_use]
    pub fn new(ctx: S) -> Self {
        Self {
            stages: [None; N],
            ctx,
            count: 0,
            validated: false,
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
    /// Receives the ordered slice of stage function pointers.
    ///
    /// # Errors
    ///
    /// Returns the error returned by `validator`, if any.
    pub fn check<F>(&self, validator: F) -> Result<(), PipelineError>
    where
        F: FnOnce(&[StageFn<S>]) -> Result<(), PipelineError>,
    {
        let fns: Vec<StageFn<S>> = self.stages[..self.count]
            .iter()
            .filter_map(|s| *s)
            .collect();
        validator(&fns)
    }

    /// Returns a reference to the pipeline's scratchpad.
    #[must_use]
    pub fn context(&self) -> &S {
        &self.ctx
    }

    /// Returns a mutable reference to the pipeline's scratchpad.
    pub fn context_mut(&mut self) -> &mut S {
        &mut self.ctx
    }

    /// Runs all stages in order against the pipeline's scratchpad.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::EmptyPipeline` if no stages have been added,
    /// `PipelineError::ValidationFailed` if the scratchpad fails validation,
    /// or the error from the first stage that fails.
    #[inline]
    pub fn run(&mut self) -> Result<(), PipelineError> {
        if self.count == 0 {
            return Err(empty_pipeline());
        }

        if !self.validated {
            if !self.ctx.validate() {
                return Err(validation_failed());
            }
            self.validated = true;
        }

        for i in 0..self.count {
            if let Some(stage) = self.stages[i] {
                stage(&mut self.ctx)?;
            }
        }

        Ok(())
    }
}

impl<S: Scratchpad, const N: usize> crate::pool::PoolablePipeline for Pipeline<S, N> {
    fn reset_for_reuse(&mut self) {
        self.ctx.reset();
        self.validated = false;
    }
}

#[cold]
#[inline(never)]
fn empty_pipeline() -> PipelineError {
    PipelineError::EmptyPipeline
}

#[cold]
#[inline(never)]
fn validation_failed() -> PipelineError {
    PipelineError::ValidationFailed(String::from(
        "scratchpad failed validation before pipeline execution",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestScratchpad {
        value: f32,
        is_valid: bool,
    }

    impl TestScratchpad {
        fn new(value: f32) -> Self {
            Self {
                value,
                is_valid: true,
            }
        }
    }

    impl Scratchpad for TestScratchpad {
        fn reset(&mut self) {
            self.value = 0.0;
        }

        fn validate(&self) -> bool {
            self.is_valid
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
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new(TestScratchpad::new(1.0));
        assert!(matches!(pipeline.run(), Err(PipelineError::EmptyPipeline)));
    }

    #[test]
    fn validation_failure_blocks_execution() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new(TestScratchpad::new(1.0));
        pipeline.add_stage(double).unwrap();
        pipeline.context_mut().is_valid = false;
        assert!(matches!(
            pipeline.run(),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn stage_runs_and_modifies_scratchpad() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new(TestScratchpad::new(2.0));
        pipeline.add_stage(double).unwrap();
        assert!(pipeline.run().is_ok());
        assert_eq!(pipeline.context().value, 4.0);
    }

    #[test]
    fn pipeline_at_capacity_returns_error() {
        let mut pipeline = Pipeline::<TestScratchpad, 1>::new(TestScratchpad::new(1.0));
        pipeline.add_stage(double).unwrap();
        let result = pipeline.add_stage(failing);
        assert!(matches!(result, Err(PipelineError::FullPipeline)));
    }

    #[test]
    fn failing_stage_returns_error() {
        let mut pipeline = Pipeline::<TestScratchpad, 1>::new(TestScratchpad::new(1.0));
        pipeline.add_stage(failing).unwrap();
        assert!(matches!(pipeline.run(), Err(PipelineError::StageFailed(_))));
    }

    #[test]
    fn contains_stage_fn_detects_added_function() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new(TestScratchpad::new(1.0));
        pipeline.add_stage(double).unwrap();
        assert!(pipeline.contains_stage_fn(double));
        assert!(!pipeline.contains_stage_fn(failing));
    }

    #[test]
    fn stage_fn_position_returns_correct_index() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new(TestScratchpad::new(1.0));
        pipeline.add_stage(double).unwrap();
        pipeline.add_stage(failing).unwrap();
        assert_eq!(pipeline.stage_fn_position(double), Some(0));
        assert_eq!(pipeline.stage_fn_position(failing), Some(1));
    }

    #[test]
    fn check_validates_stage_ordering() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new(TestScratchpad::new(1.0));
        pipeline.add_stage(double).unwrap();
        pipeline.add_stage(failing).unwrap();
        assert!(
            pipeline
                .check(|fns| {
                    let double_pos = fns
                        .iter()
                        .position(|f| std::ptr::fn_addr_eq(*f, double as StageFn<TestScratchpad>));
                    let fail_pos = fns
                        .iter()
                        .position(|f| std::ptr::fn_addr_eq(*f, failing as StageFn<TestScratchpad>));
                    match (double_pos, fail_pos) {
                        (Some(d), Some(f)) if d < f => Ok(()),
                        _ => Err(PipelineError::InvalidState("wrong order".into())),
                    }
                })
                .is_ok()
        );
    }
}
