//! Static pipeline executor using function pointers for zero heap allocation.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;

type StageFn<S> = fn(&mut S) -> Result<(), PipelineError>;

/// A fixed-capacity pipeline that stores stages as function pointers.
///
/// Unlike `DynamicPipeline`, no heap allocation occurs after initialisation.
/// The number of stages `N` must be known at compile time.
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
/// let mut pipeline = Pipeline::<MyScratchpad, 1>::new();
/// pipeline.add_stage(double);
/// ```
pub struct Pipeline<S: Scratchpad, const N: usize> {
    stages: [Option<StageFn<S>>; N],
    count: usize,
    validated: bool,
}

impl<S: Scratchpad, const N: usize> Pipeline<S, N> {
    /// Creates a new empty static pipeline with capacity for `N` stages.
    pub fn new() -> Self {
        Self {
            stages: [None; N],
            count: 0,
            validated: false,
        }
    }

    /// Adds a stage function to the pipeline.
    ///
    /// Returns an error if the pipeline is already at capacity.
    pub fn add_stage(&mut self, stage: StageFn<S>) -> Result<(), PipelineError> {
        if self.count >= N {
            return Err(PipelineError::StageFailed(String::from(
                "pipeline is at capacity",
            )));
        }
        self.stages[self.count] = Some(stage);
        self.count += 1;
        Ok(())
    }

    /// Runs all stages in order against the provided scratchpad.
    #[inline]
    pub fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        if self.count == 0 {
            return Err(PipelineError::EmptyPipeline);
        }

        if !self.validated {
            if !ctx.validate() {
                return Err(PipelineError::ValidationFailed(String::from(
                    "scratchpad failed validation before pipeline execution",
                )));
            }
            self.validated = true;
        }

        for i in 0..self.count {
            if let Some(stage) = self.stages[i] {
                stage(ctx).map_err(|e| PipelineError::StageFailed(format!("{:?}", e)))?;
            }
        }

        Ok(())
    }
}

impl<S: Scratchpad, const N: usize> Default for Pipeline<S, N> {
    fn default() -> Self {
        Self::new()
    }
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

    fn double(ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
        ctx.value *= 2.0;
        Ok(())
    }

    fn failing(_ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
        Err(PipelineError::StageFailed(String::from(
            "intentional failure",
        )))
    }

    #[test]
    fn empty_pipeline_returns_error() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new();
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::EmptyPipeline)
        ));
    }

    #[test]
    fn validation_failure_blocks_execution() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new();
        pipeline.add_stage(double).unwrap();
        let mut ctx = TestScratchpad::new(1.0);
        ctx.is_valid = false;
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn stage_runs_and_modifies_scratchpad() {
        let mut pipeline = Pipeline::<TestScratchpad, 2>::new();
        pipeline.add_stage(double).unwrap();
        let mut ctx = TestScratchpad::new(2.0);
        assert!(pipeline.run(&mut ctx).is_ok());
        assert_eq!(ctx.value, 4.0);
    }

    #[test]
    fn pipeline_at_capacity_returns_error() {
        let mut pipeline = Pipeline::<TestScratchpad, 1>::new();
        pipeline.add_stage(double).unwrap();
        let result = pipeline.add_stage(failing);
        assert!(matches!(result, Err(PipelineError::StageFailed(_))));
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
}
