//! Retry wrapper for individual pipeline stages.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// Wraps a stage with retry logic.
///
/// Before each attempt the scratchpad is snapshotted via [`Clone`]. On
/// failure it is restored to exactly the state it was in before that
/// attempt, so only the retried stage's writes are undone and earlier
/// stages' output is preserved across retries.
///
/// `max_attempts` is the **total** number of times the stage may run.
/// `Retry::new(stage, 3)` runs the stage at most 3 times.
///
/// # Allocation
///
/// `Retry` clones the scratchpad before each attempt. For scratchpads that
/// contain heap-allocated data (`Vec`, `String`, etc.) this allocates on
/// every attempt, including the first. `Retry` is intentionally excluded
/// from pipex's zero-allocation guarantee and should not be used on
/// zero-allocation hot paths.
///
/// # Example
/// ```
/// use pipex::retry::Retry;
/// use pipex::stage::Stage;
/// use pipex::scratchpad::Scratchpad;
/// use pipex::error::PipelineError;
/// use pipex::dynamic_pipeline::Pipeline;
///
/// #[derive(Clone)]
/// struct MyScratchpad { value: f32 }
///
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) { self.value = 0.0; }
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
/// let mut pipeline = Pipeline::new().stage(Retry::new(DoubleValues, 3));
/// let mut ctx = MyScratchpad { value: 1.0 };
/// pipeline.run(&mut ctx).unwrap();
/// ```
#[derive(Debug)]
pub struct Retry<S: Scratchpad, T: Stage<S>> {
    stage: T,
    max_attempts: u32,
    _marker: std::marker::PhantomData<fn(S) -> S>,
}

impl<S: Scratchpad, T: Stage<S>> Retry<S, T> {
    /// Wraps `stage` with up to `max_attempts` total executions.
    ///
    /// On each failure the scratchpad is restored to its pre-attempt state
    /// before the next attempt begins. Requires `S: Clone`.
    #[must_use]
    pub fn new(stage: T, max_attempts: u32) -> Self {
        Self {
            stage,
            max_attempts,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S: Scratchpad + Send + Clone, T: Stage<S>> Stage<S> for Retry<S, T> {
    fn name(&self) -> &'static str {
        self.stage.name()
    }

    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        let mut last_error = None;

        for attempt in 0..self.max_attempts {
            let snapshot = ctx.clone();
            match self.stage.run(ctx) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    if attempt + 1 < self.max_attempts {
                        *ctx = snapshot;
                    }
                }
            }
        }

        Err(PipelineError::RetryExhausted {
            attempts: self.max_attempts,
            source: Box::new(
                last_error.expect("last_error is Some after at least one loop iteration"),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
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
    fn successful_stage_runs_once() {
        let mut stage = Retry::new(DoubleStage, 3);
        let mut ctx = TestScratchpad::new(2.0);
        assert!(stage.run(&mut ctx).is_ok());
        assert_eq!(ctx.value, 4.0);
    }

    #[test]
    fn failing_stage_exhausts_attempts() {
        let mut stage = Retry::new(FailingStage, 3);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::RetryExhausted { attempts: 3, .. })
        ));
    }

    #[test]
    fn scratchpad_is_restored_between_attempts() {
        // A stage that writes to the scratchpad then fails.
        struct WriteAndFail;
        impl Stage<TestScratchpad> for WriteAndFail {
            fn run(&mut self, ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
                ctx.value += 100.0;
                Err(PipelineError::StageFailed(String::from("fail")))
            }
        }

        let mut stage = Retry::new(WriteAndFail, 3);
        let mut ctx = TestScratchpad::new(5.0);
        stage.run(&mut ctx).ok();
        // Each attempt restored the scratchpad before retrying, so the final
        // value reflects only the last (failed) attempt's write.
        assert_eq!(ctx.value, 105.0);
    }

    #[test]
    fn earlier_stage_output_is_preserved_across_retries() {
        // Simulate: earlier stage wrote value=10, retried stage fails.
        let mut stage = Retry::new(FailingStage, 3);
        let mut ctx = TestScratchpad::new(10.0);
        stage.run(&mut ctx).ok();
        // reset() was never called; the value from earlier stages is untouched.
        assert_eq!(ctx.value, 10.0);
    }

    #[test]
    fn max_attempts_of_one_fails_immediately() {
        let mut stage = Retry::new(FailingStage, 1);
        let mut ctx = TestScratchpad::new(1.0);
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::RetryExhausted { attempts: 1, .. })
        ));
    }
}
