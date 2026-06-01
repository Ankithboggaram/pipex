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
/// The pipeline owns its scratchpad. Use [`context`][Pipeline::context] and
/// [`context_mut`][Pipeline::context_mut] to write inputs and read outputs.
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
/// let pipeline: Pipeline<MyScratchpad> = Pipeline::new(MyScratchpad);
/// ```
pub struct Pipeline<S: Scratchpad> {
    // The sequence of stages to execute.
    stages: Vec<Box<dyn Stage<S>>>,
    ctx: S,
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

impl<S: Scratchpad> Pipeline<S> {
    /// Creates a new empty pipeline with the given scratchpad.
    #[must_use]
    pub fn new(ctx: S) -> Self {
        Self {
            stages: Vec::new(),
            ctx,
            validated: false,
        }
    }

    /// Adds a stage and returns the pipeline for chaining.
    #[must_use]
    pub fn stage<T: Stage<S> + 'static>(mut self, stage: T) -> Self {
        self.stages.push(Box::new(stage));
        self
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
        if self.stages.is_empty() {
            return Err(empty_pipeline());
        }

        if !self.validated {
            if !self.ctx.validate() {
                return Err(validation_failed());
            }
            self.validated = true;
        }

        for i in 0..self.stages.len() {
            self.stages[i].run(&mut self.ctx)?;
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
        let mut pipeline = Pipeline::new(TestScratchpad::new(1.0));
        assert!(matches!(pipeline.run(), Err(PipelineError::EmptyPipeline)));
    }

    #[test]
    fn validation_failure_blocks_execution() {
        let mut pipeline = Pipeline::new(TestScratchpad::new(1.0)).stage(DoubleStage);
        pipeline.context_mut().is_valid = false;
        assert!(matches!(
            pipeline.run(),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn stage_runs_and_modifies_scratchpad() {
        let mut pipeline = Pipeline::new(TestScratchpad::new(2.0)).stage(DoubleStage);
        assert!(pipeline.run().is_ok());
        assert_eq!(pipeline.context().value, 4.0);
    }

    #[test]
    fn failing_stage_returns_error() {
        let mut pipeline = Pipeline::new(TestScratchpad::new(1.0)).stage(FailingStage);
        assert!(matches!(pipeline.run(), Err(PipelineError::StageFailed(_))));
    }
}
