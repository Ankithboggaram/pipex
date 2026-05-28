//! Retry wrapper for individual pipeline stages.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// Wraps a stage with retry logic.
///
/// On failure, the scratchpad is reset and the stage is retried up to
/// `retries` times before returning `PipelineError::RetryExhausted`.
///
/// # Example
/// ```
/// use pipex::retry::Retry;
/// use pipex::stage::Stage;
/// use pipex::scratchpad::Scratchpad;
/// use pipex::error::PipelineError;
/// use pipex::dynamic_pipeline::Pipeline;
///
/// struct MyScratchpad { value: f32 }
///
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) { self.value = 0.0; }
///     fn validate(&self) -> bool { true }
/// }
///
/// struct DoubleValues;
///
/// impl Stage<MyScratchpad> for DoubleValues {
///     fn run(&mut self, ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
///         ctx.value *= 2.0;
///         Ok(())
///     }
/// }
///
/// let mut pipeline = Pipeline::new();
/// pipeline.add_stage(Retry::new(DoubleValues, 3));
/// ```
#[derive(Debug)]
pub struct Retry<S: Scratchpad, T: Stage<S>> {
    stage: T,
    retries: u32,
    _marker: std::marker::PhantomData<S>,
}

impl<S: Scratchpad, T: Stage<S>> Retry<S, T> {
    /// Creates a new `Retry` wrapper around a stage with a given retry count.
    #[must_use]
    pub fn new(stage: T, retries: u32) -> Self {
        Self {
            stage,
            retries,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S: Scratchpad, T: Stage<S>> Stage<S> for Retry<S, T> {
    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        let mut last_error = None;

        for attempt in 0..=self.retries {
            match self.stage.run(ctx) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.retries {
                        ctx.reset();
                    }
                }
            }
        }

        Err(PipelineError::RetryExhausted {
            attempts: self.retries + 1,
            reason: format!(
                "{:?}",
                last_error.expect("last_error is Some after at least one loop iteration")
            ),
        })
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
    fn successful_stage_runs_without_retry() {
        let mut stage = Retry::new(DoubleStage, 3);
        let mut ctx = TestScratchpad::new(2.0);
        assert!(stage.run(&mut ctx).is_ok());
        assert_eq!(ctx.value, 4.0);
    }

    #[test]
    fn failing_stage_exhausts_retries() {
        let mut stage = Retry::new(FailingStage, 2);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::RetryExhausted { attempts: 3, .. })
        ));
    }

    #[test]
    fn scratchpad_is_reset_between_retries() {
        let mut stage = Retry::new(FailingStage, 2);
        let mut ctx = TestScratchpad::new(5.0);
        stage.run(&mut ctx).ok();
        assert_eq!(ctx.value, 0.0);
    }

    #[test]
    fn retry_with_zero_retries_fails_immediately() {
        let mut stage = Retry::new(FailingStage, 0);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::RetryExhausted { attempts: 1, .. })
        ));
    }
}
