//! Tracing span instrumentation wrapper for pipeline stages.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// Wraps a stage with a tracing span, emitting structured observability
/// data on every execution.
///
/// Integrates with the `tracing` ecosystem. Spans are routed to whatever
/// subscriber the downstream application configures (terminal, Jaeger,
/// OpenTelemetry, etc.).
///
/// # Example
/// ```
/// use pipex::instrument::Instrumented;
/// use pipex::stage::Stage;
/// use pipex::scratchpad::Scratchpad;
/// use pipex::error::PipelineError;
/// use pipex::dynamic_pipeline::Pipeline;
///
/// struct MyScratchpad;
///
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) {}
/// }
///
/// struct MyStage;
///
/// impl Stage<MyScratchpad> for MyStage {
///     fn run(&mut self, _ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
///         Ok(())
///     }
/// }
///
/// let mut pipeline = Pipeline::new().stage(Instrumented::new(MyStage, "my_stage"));
/// let mut ctx = MyScratchpad;
/// ```
#[derive(Debug)]
pub struct Instrumented<S: Scratchpad, T: Stage<S>> {
    stage: T,
    name: &'static str,
    _marker: std::marker::PhantomData<fn(S) -> S>,
}

impl<S: Scratchpad, T: Stage<S>> Instrumented<S, T> {
    /// Creates a new `Instrumented` wrapper around a stage.
    ///
    /// The `name` parameter must be a `&'static str` known at compile time.
    #[must_use]
    pub fn new(stage: T, name: &'static str) -> Self {
        Self {
            stage,
            name,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S: Scratchpad + Send, T: Stage<S>> Stage<S> for Instrumented<S, T> {
    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        let span = tracing::info_span!("stage", name = self.name);
        let _enter = span.enter();

        let result = self.stage.run(ctx);

        if let Err(ref e) = result {
            tracing::error!(name = self.name, error = ?e, "stage failed");
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

    struct NoopStage;

    impl Stage<TestScratchpad> for NoopStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            Ok(())
        }
    }

    struct FailStage;

    impl Stage<TestScratchpad> for FailStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            Err(PipelineError::StageFailed(String::from("fail")))
        }
    }

    #[test]
    fn instrumented_stage_succeeds() {
        let mut stage = Instrumented::new(NoopStage, "noop");
        let mut ctx = TestScratchpad;
        assert!(stage.run(&mut ctx).is_ok());
    }

    #[test]
    fn instrumented_stage_propagates_error() {
        let mut stage = Instrumented::new(FailStage, "fail");
        let mut ctx = TestScratchpad;
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn instrumented_and_timed_can_compose() {
        use crate::metrics::{StageMetrics, Timed};
        use std::sync::Arc;

        let metrics = StageMetrics::new("noop");
        let mut stage = Timed::new(Instrumented::new(NoopStage, "noop"), Arc::clone(&metrics));
        let mut ctx = TestScratchpad;

        stage.run(&mut ctx).unwrap();

        assert_eq!(metrics.snapshot().count, 1);
    }
}
