//! Dynamic pipeline executor using boxed trait objects for runtime flexibility.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// A pipeline that stores stages as boxed trait objects, allowing different
/// stage types to be mixed at runtime.
///
/// Uses dynamic dispatch via `Box<dyn Stage<S>>`. For a zero heap allocation
/// alternative with stages known at compile time, see `StaticPipeline`.
///
/// # Example
/// ```
/// use pipex::dynamic_pipeline::Pipeline;
/// use pipex::scratchpad::Scratchpad;
///
/// struct MyScratchpad;
///
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) {}
///     fn validate(&self) -> bool { true }
/// }
///
/// let pipeline: Pipeline<MyScratchpad> = Pipeline::new()
///     .with_retries(3);
/// ```
pub struct Pipeline<S: Scratchpad> {
    /// The sequence of stages to execute.
    stages: Vec<Box<dyn Stage<S>>>,
    /// The number of times to retry a failed stage before giving up.
    retries: u32,
}

impl<S: Scratchpad> Default for Pipeline<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Scratchpad> Pipeline<S> {
    /// Creates a new empty pipeline with no retries.
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            retries: 0,
        }
    }

    /// Sets the number of retries for failed stages.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Adds a stage to the pipeline.
    pub fn add_stage<T: Stage<S> + 'static>(&mut self, stage: T) {
        self.stages.push(Box::new(stage));
    }

    /// Runs all stages in order against the provided scratchpad.
    ///
    /// Validates the scratchpad before execution, and resets it
    /// between pipeline runs.
    pub fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        if self.stages.is_empty() {
            return Err(PipelineError::EmptyPipeline);
        }

        if !ctx.validate() {
            return Err(PipelineError::ValidationFailed(String::from(
                "Scratchpad failed validation before pipeline execution",
            )));
        }

        for stage in self.stages.iter_mut() {
            if self.retries == 0 {
                stage
                    .run(ctx)
                    .map_err(|e| PipelineError::StageFailed(format!("{:?}", e)))?;
            } else {
                let mut last_error = None;

                for attempt in 0..=self.retries {
                    match stage.run(ctx) {
                        Ok(()) => {
                            last_error = None;
                            break;
                        }
                        Err(e) => {
                            last_error = Some(e);
                            if attempt < self.retries {
                                ctx.reset();
                            }
                        }
                    }
                }

                if let Some(e) = last_error {
                    return Err(PipelineError::RetryExhausted {
                        attempts: self.retries + 1,
                        reason: format!("{:?}", e),
                    });
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A minimal scratchpad for testing
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

    // A stage that doubles the value
    struct DoubleStage;

    impl Stage<TestScratchpad> for DoubleStage {
        fn run(&mut self, ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            ctx.value *= 2.0;
            Ok(())
        }
    }

    // A stage that always fails
    struct FailingStage;

    impl Stage<TestScratchpad> for FailingStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            Err(PipelineError::StageFailed(String::from(
                "intentional failure",
            )))
        }
    }

    #[test]
    fn empty_pipeline_returns_error() {
        let mut pipeline: Pipeline<TestScratchpad> = Pipeline::new();
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::EmptyPipeline)
        ));
    }

    #[test]
    fn validation_failure_blocks_execution() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(DoubleStage);
        let mut ctx = TestScratchpad::new(1.0);
        ctx.is_valid = false;
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn stage_runs_and_modifies_scratchpad() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(DoubleStage);
        let mut ctx = TestScratchpad::new(2.0);
        assert!(pipeline.run(&mut ctx).is_ok());
        assert_eq!(ctx.value, 4.0);
    }

    #[test]
    fn failing_stage_exhausts_retries() {
        let mut pipeline = Pipeline::new().with_retries(2);
        pipeline.add_stage(FailingStage);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::RetryExhausted { attempts: 3, .. })
        ));
    }

    #[test]
    fn failing_stage_with_no_retries_returns_stage_failed() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(FailingStage);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }
}
