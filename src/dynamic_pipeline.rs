//! Dynamic pipeline executor using boxed trait objects for runtime flexibility.

use std::any::TypeId;

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// A pipeline that stores stages as boxed trait objects, allowing different
/// stage types to be mixed at runtime.
///
/// Uses dynamic dispatch via `Box<dyn Stage<S>>`. For a zero heap allocation
/// alternative with stages known at compile time, see
/// [`static_pipeline::Pipeline`][crate::static_pipeline::Pipeline].
///
/// The pipeline holds no data — stages are run by passing a mutable scratchpad
/// reference to [`run`][Pipeline::run].
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
    stages: Vec<Box<dyn Stage<S>>>,
    stage_type_ids: Vec<TypeId>,
}

impl<S: Scratchpad> Default for Pipeline<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Scratchpad> std::fmt::Debug for Pipeline<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("stages", &self.stages.len())
            .finish()
    }
}

impl<S: Scratchpad> Pipeline<S> {
    /// Creates a new empty pipeline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            stage_type_ids: Vec::new(),
        }
    }

    /// Adds a stage and returns the pipeline for chaining.
    #[must_use]
    pub fn stage<T: Stage<S> + 'static>(mut self, stage: T) -> Self {
        self.stage_type_ids.push(TypeId::of::<T>());
        self.stages.push(Box::new(stage));
        self
    }

    /// Adds a stage to the pipeline.
    pub fn add_stage<T: Stage<S> + 'static>(&mut self, stage: T) {
        self.stage_type_ids.push(TypeId::of::<T>());
        self.stages.push(Box::new(stage));
    }

    /// Adds a pre-boxed stage directly, avoiding a double-box allocation.
    ///
    /// Intended for downstream wiring code that builds wrapped stages
    /// incrementally before handing them to the pipeline. Stage type
    /// information is not tracked for stages added via this method.
    pub fn push_boxed(&mut self, stage: Box<dyn Stage<S>>) {
        self.stages.push(stage);
    }

    /// Returns `true` if the pipeline contains a stage of type `T`.
    ///
    /// Only tracks stages added via [`stage`][Pipeline::stage] or
    /// [`add_stage`][Pipeline::add_stage], not [`push_boxed`][Pipeline::push_boxed].
    #[must_use]
    pub fn contains_stage<T: Stage<S> + 'static>(&self) -> bool {
        self.stage_type_ids.contains(&TypeId::of::<T>())
    }

    /// Returns the index of the first stage of type `T`, or `None` if not present.
    ///
    /// Only tracks stages added via [`stage`][Pipeline::stage] or
    /// [`add_stage`][Pipeline::add_stage], not [`push_boxed`][Pipeline::push_boxed].
    #[must_use]
    pub fn stage_position<T: Stage<S> + 'static>(&self) -> Option<usize> {
        self.stage_type_ids
            .iter()
            .position(|id| *id == TypeId::of::<T>())
    }

    /// Validates the pipeline's stage configuration with a user-provided closure.
    ///
    /// Receives the ordered slice of [`TypeId`]s for all tracked stages.
    ///
    /// # Errors
    ///
    /// Returns the error returned by `validator`, if any.
    pub fn check<F>(&self, validator: F) -> Result<(), PipelineError>
    where
        F: FnOnce(&[TypeId]) -> Result<(), PipelineError>,
    {
        validator(&self.stage_type_ids)
    }

    /// Runs all stages in order against the provided scratchpad.
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

        if !ctx.validate() {
            return Err(validation_failed());
        }

        for i in 0..self.stages.len() {
            self.stages[i].run(ctx)?;
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

    struct StageA;
    impl Stage<TestScratchpad> for StageA {
        fn run(&mut self, ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            ctx.value *= 2.0;
            Ok(())
        }
    }

    struct StageB;
    impl Stage<TestScratchpad> for StageB {
        fn run(&mut self, ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            ctx.value += 1.0;
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
        let mut pipeline = Pipeline::new();
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::EmptyPipeline)
        ));
    }

    #[test]
    fn validation_failure_blocks_execution() {
        let mut pipeline = Pipeline::new().stage(StageA);
        let mut ctx = TestScratchpad::new(1.0);
        ctx.is_valid = false;
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn stage_runs_and_modifies_scratchpad() {
        let mut pipeline = Pipeline::new().stage(StageA);
        let mut ctx = TestScratchpad::new(2.0);
        assert!(pipeline.run(&mut ctx).is_ok());
        assert_eq!(ctx.value, 4.0);
    }

    #[test]
    fn failing_stage_returns_error() {
        let mut pipeline = Pipeline::new().stage(FailingStage);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn contains_stage_detects_added_type() {
        let pipeline = Pipeline::new().stage(StageA).stage(StageB);
        assert!(pipeline.contains_stage::<StageA>());
        assert!(pipeline.contains_stage::<StageB>());
        assert!(!pipeline.contains_stage::<FailingStage>());
    }

    #[test]
    fn stage_position_returns_correct_index() {
        let pipeline = Pipeline::new().stage(StageA).stage(StageB);
        assert_eq!(pipeline.stage_position::<StageA>(), Some(0));
        assert_eq!(pipeline.stage_position::<StageB>(), Some(1));
        assert_eq!(pipeline.stage_position::<FailingStage>(), None);
    }

    #[test]
    fn check_passes_with_valid_ordering() {
        let pipeline = Pipeline::new().stage(StageA).stage(StageB);
        assert!(
            pipeline
                .check(|ids| {
                    let a = ids
                        .iter()
                        .position(|id| *id == TypeId::of::<StageA>())
                        .unwrap();
                    let b = ids
                        .iter()
                        .position(|id| *id == TypeId::of::<StageB>())
                        .unwrap();
                    if a < b {
                        Ok(())
                    } else {
                        Err(PipelineError::InvalidState("wrong order".into()))
                    }
                })
                .is_ok()
        );
    }

    #[test]
    fn check_fails_with_invalid_ordering() {
        let pipeline = Pipeline::new().stage(StageB).stage(StageA);
        assert!(
            pipeline
                .check(|ids| {
                    let a = ids
                        .iter()
                        .position(|id| *id == TypeId::of::<StageA>())
                        .unwrap();
                    let b = ids
                        .iter()
                        .position(|id| *id == TypeId::of::<StageB>())
                        .unwrap();
                    if a < b {
                        Ok(())
                    } else {
                        Err(PipelineError::InvalidState("wrong order".into()))
                    }
                })
                .is_err()
        );
    }
}
