//! Tracing span instrumentation for pipeline stages.
//!
//! Wrap any stage in [`Instrumented`] to emit a [`tracing`] span on every
//! execution. The span name is derived from [`Stage::name`][crate::stage::Stage]
//! and routed to whatever subscriber the application configures.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// Wraps a stage with a tracing span, emitting structured observability
/// data on every execution.
///
/// The span name is derived from [`Stage::name`]; override that method on
/// your stage type to customise it. No explicit name is required at
/// construction time.
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
/// let mut pipeline = Pipeline::new().stage(Instrumented::new(MyStage));
/// let mut ctx = MyScratchpad;
/// ```
#[derive(Debug)]
pub struct Instrumented<S: Scratchpad, T: Stage<S>> {
    stage: T,
    _marker: std::marker::PhantomData<fn(S) -> S>,
}

impl<S: Scratchpad, T: Stage<S>> Instrumented<S, T> {
    /// Wraps `stage` in a tracing span.
    ///
    /// The span name comes from [`Stage::name`] on the inner stage.
    /// Override it there if you need a custom label.
    #[must_use]
    pub fn new(stage: T) -> Self {
        Self {
            stage,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S: Scratchpad + Send, T: Stage<S>> Stage<S> for Instrumented<S, T> {
    fn name(&self) -> &'static str {
        self.stage.name()
    }

    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        let name = self.stage.name();
        let span = tracing::info_span!("stage", name);
        let _enter = span.enter();

        let result = self.stage.run(ctx);

        if let Err(ref e) = result {
            tracing::error!(name, error = ?e, "stage failed");
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
            Err(PipelineError::StageFailed {
                stage: "FailStage",
                message: String::from("fail"),
            })
        }
    }

    #[test]
    fn instrumented_stage_succeeds() {
        let mut stage = Instrumented::new(NoopStage);
        let mut ctx = TestScratchpad;
        assert!(stage.run(&mut ctx).is_ok());
    }

    #[test]
    fn instrumented_stage_propagates_error() {
        let mut stage = Instrumented::new(FailStage);
        let mut ctx = TestScratchpad;
        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::StageFailed { .. })
        ));
    }

    #[test]
    fn name_delegates_to_inner_stage() {
        let stage = Instrumented::new(NoopStage);
        assert!(stage.name().contains("NoopStage"));
    }

    #[test]
    fn instrumented_and_timed_can_compose() {
        use crate::metrics::Timed;

        let (mut stage, noop_metrics) = Timed::new(Instrumented::new(NoopStage));
        let mut ctx = TestScratchpad;

        stage.run(&mut ctx).unwrap();

        assert_eq!(noop_metrics.snapshot().count, 1);
    }
}
