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
/// let pipeline: Pipeline<MyScratchpad> = Pipeline::new();
/// ```
pub struct Pipeline<S: Scratchpad> {
    // The sequence of stages to execute.
    stages: Vec<Box<dyn Stage<S>>>,
    validated: bool,
}

impl<S: Scratchpad> std::fmt::Debug for Pipeline<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("stages", &self.stages.len())
            .field("validated", &self.validated)
            .finish()
    }
}

impl<S: Scratchpad> Default for Pipeline<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Scratchpad> Pipeline<S> {
    /// Creates a new empty pipeline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            validated: false,
        }
    }

    /// Adds a stage to the pipeline.
    pub fn add_stage<T: Stage<S> + 'static>(&mut self, stage: T) {
        self.stages.push(Box::new(stage));
    }

    /// Adds a pre-boxed stage directly, avoiding a double-box allocation.
    ///
    /// Intended for downstream wiring code that builds wrapped stages
    /// incrementally before handing them to the pipeline.
    pub fn push_boxed(&mut self, stage: Box<dyn Stage<S>>) {
        self.stages.push(stage);
    }

    /// Runs all stages in order against the provided scratchpad.
    ///
    /// Validates the scratchpad before execution, and resets it
    /// between pipeline runs.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::EmptyPipeline` if no stages have been added,
    /// `PipelineError::ValidationFailed` if the scratchpad fails validation,
    /// or the error from the first stage that fails.
    #[inline]
    pub fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        if self.stages.is_empty() {
            return Err(empty_pipeline());
        }

        if !self.validated {
            if !ctx.validate() {
                return Err(validation_failed());
            }
            self.validated = true;
        }

        for stage in &mut self.stages {
            stage.run(ctx)?;
        }

        Ok(())
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

    struct DoubleStage;

    impl Stage<TestScratchpad> for DoubleStage {
        fn run(&mut self, ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            ctx.value *= 2.0;
            Ok(())
        }
    }

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
    fn failing_stage_returns_error() {
        let mut pipeline = Pipeline::new();
        pipeline.add_stage(FailingStage);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }
}
