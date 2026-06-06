//! Deadline wrapper for surfacing latency violations as pipeline errors.
//!
//! [`Deadline`] is a post-hoc guard, not a preemptive timeout: the stage runs
//! to completion and elapsed time is checked afterwards. Use it for SLA
//! enforcement and circuit-breaking, not for aborting slow stages mid-flight.

use std::marker::PhantomData;
use std::time::{Duration, Instant};

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// Wraps a stage with a time budget.
///
/// If the stage completes successfully within the budget, the result is
/// returned unchanged. If the stage succeeds but exceeds the budget,
/// `PipelineError::DeadlineExceeded` is returned. If the stage fails,
/// its error is returned regardless of elapsed time.
///
/// Note: this is a deadline guard, not a preemptive timeout. The stage
/// runs to completion on the current thread. Execution cannot be
/// interrupted mid-flight. Use this to surface latency violations as
/// pipeline errors and enable circuit breaking or SLA enforcement.
///
/// # Example
/// ```
/// use pipex::deadline::Deadline;
/// use pipex::dynamic_pipeline::Pipeline;
/// use pipex::scratchpad::Scratchpad;
/// use pipex::stage::Stage;
/// use pipex::error::PipelineError;
/// use std::time::Duration;
///
/// struct MyScratchpad;
///
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) {}
/// }
///
/// struct FastStage;
///
/// impl Stage<MyScratchpad> for FastStage {
///     fn run(&mut self, _ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
///         Ok(())
///     }
/// }
///
/// let mut pipeline = Pipeline::new().stage(Deadline::new(FastStage, Duration::from_millis(100)));
/// let mut ctx = MyScratchpad;
/// assert!(pipeline.run(&mut ctx).is_ok());
/// ```
#[derive(Debug)]
pub struct Deadline<S: Scratchpad, T: Stage<S>> {
    stage: T,
    budget: Duration,
    _marker: PhantomData<fn(S) -> S>,
}

impl<S: Scratchpad, T: Stage<S>> Deadline<S, T> {
    /// Creates a new `Deadline` wrapper with the given time budget.
    #[must_use]
    pub fn new(stage: T, budget: Duration) -> Self {
        Self {
            stage,
            budget,
            _marker: PhantomData,
        }
    }
}

impl<S: Scratchpad, T: Stage<S>> Stage<S> for Deadline<S, T> {
    fn name(&self) -> &'static str {
        self.stage.name()
    }

    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        let start = Instant::now();
        let result = self.stage.run(ctx);
        let elapsed = start.elapsed();

        if result.is_ok() && elapsed > self.budget {
            return Err(PipelineError::DeadlineExceeded {
                budget_ns: self.budget.as_nanos() as u64,
                elapsed_ns: elapsed.as_nanos() as u64,
            });
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestScratchpad;

    impl Scratchpad for TestScratchpad {
        fn reset(&mut self) {}
    }

    struct FastStage;

    impl Stage<TestScratchpad> for FastStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            Ok(())
        }
    }

    struct SlowStage;

    impl Stage<TestScratchpad> for SlowStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            std::thread::sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    struct FailingStage;

    impl Stage<TestScratchpad> for FailingStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            Err(PipelineError::StageFailed {
                stage: "FailingStage",
                message: String::from("intentional failure"),
            })
        }
    }

    #[test]
    fn fast_stage_within_budget_succeeds() {
        let mut stage = Deadline::new(FastStage, Duration::from_secs(1));
        let mut ctx = TestScratchpad;
        assert!(stage.run(&mut ctx).is_ok());
    }

    #[test]
    fn slow_stage_exceeding_budget_returns_deadline_error() {
        let mut stage = Deadline::new(SlowStage, Duration::from_millis(1));
        let mut ctx = TestScratchpad;
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::DeadlineExceeded { .. })
        ));
    }

    #[test]
    fn failing_stage_returns_stage_error_regardless_of_time() {
        let mut stage = Deadline::new(FailingStage, Duration::from_nanos(1));
        let mut ctx = TestScratchpad;
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::StageFailed { .. })
        ));
    }

    #[test]
    fn deadline_error_carries_budget_and_elapsed() {
        let mut stage = Deadline::new(SlowStage, Duration::from_millis(1));
        let mut ctx = TestScratchpad;
        if let Err(PipelineError::DeadlineExceeded {
            budget_ns,
            elapsed_ns,
        }) = stage.run(&mut ctx)
        {
            assert_eq!(budget_ns, 1_000_000);
            assert!(elapsed_ns > budget_ns);
        }
    }
}
