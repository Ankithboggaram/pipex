use pipex::deadline::Deadline;
use pipex::dynamic_pipeline::Pipeline as DynamicPipeline;
use pipex::error::PipelineError;
use pipex::metrics::{PipelineMetrics, Timed};
use pipex::pool::PipelinePool;
use pipex::retry::Retry;
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::static_pipeline::Pipeline as StaticPipeline;
use std::sync::Arc;
use std::time::Duration;

struct MlScratchpad {
    raw: Vec<f32>,
    normalised: Vec<f32>,
    clamped: Vec<f32>,
}

impl MlScratchpad {
    fn new(data: Vec<f32>) -> Self {
        let len = data.len();
        Self {
            raw: data,
            normalised: vec![0.0; len],
            clamped: vec![0.0; len],
        }
    }
}

impl Scratchpad for MlScratchpad {
    fn reset(&mut self) {
        self.normalised.iter_mut().for_each(|x| *x = 0.0);
        self.clamped.iter_mut().for_each(|x| *x = 0.0);
    }

    fn validate(&self) -> bool {
        !self.raw.is_empty()
    }
}

struct NormaliseStage;

impl Stage<MlScratchpad> for NormaliseStage {
    fn run(&mut self, ctx: &mut MlScratchpad) -> Result<(), PipelineError> {
        let max = ctx.raw.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        if max == 0.0 {
            return Err(PipelineError::StageFailed(String::from(
                "cannot normalise: max value is zero",
            )));
        }
        ctx.normalised
            .iter_mut()
            .zip(ctx.raw.iter())
            .for_each(|(n, r)| *n = r / max);
        Ok(())
    }
}

struct ClampStage;

impl Stage<MlScratchpad> for ClampStage {
    fn run(&mut self, ctx: &mut MlScratchpad) -> Result<(), PipelineError> {
        ctx.clamped
            .iter_mut()
            .zip(ctx.normalised.iter())
            .for_each(|(c, n)| *c = n.clamp(0.1, 0.9));
        Ok(())
    }
}

struct AlwaysFailStage;

impl Stage<MlScratchpad> for AlwaysFailStage {
    fn run(&mut self, _ctx: &mut MlScratchpad) -> Result<(), PipelineError> {
        Err(PipelineError::StageFailed(String::from(
            "intentional failure",
        )))
    }
}

#[allow(clippy::unnecessary_wraps)]
fn normalise(ctx: &mut MlScratchpad) -> Result<(), PipelineError> {
    let max = ctx.raw.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    if max == 0.0 {
        return Err(PipelineError::StageFailed(String::from(
            "cannot normalise: max value is zero",
        )));
    }
    ctx.normalised
        .iter_mut()
        .zip(ctx.raw.iter())
        .for_each(|(n, r)| *n = r / max);
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn clamp(ctx: &mut MlScratchpad) -> Result<(), PipelineError> {
    ctx.clamped
        .iter_mut()
        .zip(ctx.normalised.iter())
        .for_each(|(c, n)| *c = n.clamp(0.1, 0.9));
    Ok(())
}

mod dynamic_pipeline_tests {
    use super::*;

    #[test]
    fn runs_stages_in_order() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0, 8.0]))
            .stage(NormaliseStage)
            .stage(ClampStage);
        pipeline.run().unwrap();

        assert!((pipeline.context().clamped[0] - 0.125).abs() < 1e-6);
        assert!((pipeline.context().clamped[1] - 0.25).abs() < 1e-6);
        assert!((pipeline.context().clamped[2] - 0.5).abs() < 1e-6);
        assert!((pipeline.context().clamped[3] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn returns_empty_pipeline_error() {
        let mut pipeline: DynamicPipeline<MlScratchpad> =
            DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0]));
        assert!(matches!(pipeline.run(), Err(PipelineError::EmptyPipeline)));
    }

    #[test]
    fn returns_validation_error_on_empty_scratchpad() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![])).stage(NormaliseStage);
        assert!(matches!(
            pipeline.run(),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn returns_stage_error_on_failure() {
        let mut pipeline =
            DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0])).stage(AlwaysFailStage);
        assert!(matches!(pipeline.run(), Err(PipelineError::StageFailed(_))));
    }

    #[test]
    fn stage_error_propagates_message() {
        let mut pipeline =
            DynamicPipeline::new(MlScratchpad::new(vec![0.0, 0.0, 0.0])).stage(NormaliseStage);
        assert!(matches!(pipeline.run(), Err(PipelineError::StageFailed(_))));
    }

    #[test]
    fn can_run_multiple_times() {
        let mut pipeline =
            DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0])).stage(NormaliseStage);
        pipeline.run().unwrap();
        let first_result = pipeline.context().normalised.clone();

        pipeline.context_mut().raw = vec![2.0, 4.0, 8.0];
        pipeline.context_mut().reset();
        pipeline.run().unwrap();

        assert_eq!(pipeline.context().normalised, first_result);
    }
}

mod static_pipeline_tests {
    use super::*;

    #[test]
    fn runs_stages_in_order() {
        let mut pipeline =
            StaticPipeline::<MlScratchpad, 2>::new(MlScratchpad::new(vec![1.0, 2.0, 4.0, 8.0]));
        pipeline.add_stage(normalise).unwrap();
        pipeline.add_stage(clamp).unwrap();
        pipeline.run().unwrap();

        assert!((pipeline.context().clamped[0] - 0.125).abs() < 1e-6);
        assert!((pipeline.context().clamped[1] - 0.25).abs() < 1e-6);
        assert!((pipeline.context().clamped[2] - 0.5).abs() < 1e-6);
        assert!((pipeline.context().clamped[3] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn returns_empty_pipeline_error() {
        let mut pipeline =
            StaticPipeline::<MlScratchpad, 2>::new(MlScratchpad::new(vec![1.0, 2.0]));
        assert!(matches!(pipeline.run(), Err(PipelineError::EmptyPipeline)));
    }

    #[test]
    fn returns_error_when_over_capacity() {
        let mut pipeline =
            StaticPipeline::<MlScratchpad, 1>::new(MlScratchpad::new(vec![1.0, 2.0]));
        pipeline.add_stage(normalise).unwrap();
        assert!(matches!(
            pipeline.add_stage(clamp),
            Err(PipelineError::FullPipeline)
        ));
    }

    #[test]
    fn returns_validation_error_on_empty_scratchpad() {
        let mut pipeline = StaticPipeline::<MlScratchpad, 1>::new(MlScratchpad::new(vec![]));
        pipeline.add_stage(normalise).unwrap();
        assert!(matches!(
            pipeline.run(),
            Err(PipelineError::ValidationFailed(_))
        ));
    }
}

mod retry_tests {
    use super::*;

    #[test]
    fn succeeds_on_first_attempt() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0]))
            .stage(Retry::new(NormaliseStage, 3));
        assert!(pipeline.run().is_ok());
    }

    #[test]
    fn exhausts_retries_on_persistent_failure() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0]))
            .stage(Retry::new(AlwaysFailStage, 2));
        let result = pipeline.run();
        assert!(matches!(result, Err(PipelineError::RetryExhausted { .. })));
    }

    #[test]
    fn resets_scratchpad_between_attempts() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0]))
            .stage(Retry::new(AlwaysFailStage, 2));
        pipeline.run().ok();
        assert!(pipeline.context().normalised.iter().all(|x| *x == 0.0));
    }
}

mod deadline_tests {
    use super::*;

    #[test]
    fn fast_stage_completes_within_budget() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0]))
            .stage(Deadline::new(NormaliseStage, Duration::from_secs(1)));
        assert!(pipeline.run().is_ok());
    }

    #[test]
    fn deadline_error_carries_budget_and_elapsed() {
        struct SlowStage;
        impl Stage<MlScratchpad> for SlowStage {
            fn run(&mut self, _ctx: &mut MlScratchpad) -> Result<(), PipelineError> {
                std::thread::sleep(Duration::from_millis(20));
                Ok(())
            }
        }

        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0]))
            .stage(Deadline::new(SlowStage, Duration::from_millis(1)));

        match pipeline.run() {
            Err(PipelineError::DeadlineExceeded {
                budget_ns,
                elapsed_ns,
            }) => {
                assert_eq!(budget_ns, 1_000_000);
                assert!(elapsed_ns > budget_ns);
            }
            other => panic!("expected DeadlineExceeded, got {other:?}"),
        }
    }

    #[test]
    fn stage_error_takes_priority_over_deadline() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0]))
            .stage(Deadline::new(AlwaysFailStage, Duration::from_nanos(1)));
        assert!(matches!(pipeline.run(), Err(PipelineError::StageFailed(_))));
    }

    #[test]
    fn deadline_composes_with_retry() {
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0])).stage(
            Retry::new(Deadline::new(NormaliseStage, Duration::from_secs(1)), 2),
        );
        assert!(pipeline.run().is_ok());
    }
}

mod pipeline_metrics_tests {
    use super::*;

    #[test]
    fn tracks_execution_count_across_stages() {
        let mut pm = PipelineMetrics::new();
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0]))
            .stage(Timed::new(NormaliseStage, pm.track("normalise")))
            .stage(Timed::new(ClampStage, pm.track("clamp")));

        pipeline.run().unwrap();

        let snapshot = pm.snapshot();
        assert_eq!(snapshot.stages.len(), 2);
        assert_eq!(snapshot.total_count(), 2);
        assert_eq!(snapshot.stages[0].count, 1);
        assert_eq!(snapshot.stages[1].count, 1);
    }

    #[test]
    fn snapshot_identifies_slowest_stage() {
        let mut pm = PipelineMetrics::new();
        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0]))
            .stage(Timed::new(NormaliseStage, pm.track("normalise")))
            .stage(Timed::new(ClampStage, pm.track("clamp")));

        for _ in 0..10 {
            pipeline.context_mut().reset();
            pipeline.run().unwrap();
        }

        let snapshot = pm.snapshot();
        assert!(snapshot.slowest_stage().is_some());
    }

    #[test]
    fn error_stages_filters_correctly() {
        let mut pm = PipelineMetrics::new();
        let normalise_m = pm.track("normalise");
        let fail_m = pm.track("always_fail");

        let mut pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0]))
            .stage(Timed::new(NormaliseStage, Arc::clone(&normalise_m)));
        pipeline.run().unwrap();

        let mut fail_stage = Timed::new(AlwaysFailStage, Arc::clone(&fail_m));
        let mut ctx = MlScratchpad::new(vec![1.0]);
        fail_stage.run(&mut ctx).ok();

        let snapshot = pm.snapshot();
        let errored: Vec<_> = snapshot.error_stages().collect();
        assert_eq!(errored.len(), 1);
        assert_eq!(errored[0].label, "always_fail");
    }
}

mod ordering_tests {
    use super::*;

    #[test]
    fn dynamic_check_passes_with_correct_ordering() {
        let pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0]))
            .stage(NormaliseStage)
            .stage(ClampStage);

        assert!(
            pipeline
                .check(|ids| {
                    let n = ids
                        .iter()
                        .position(|id| *id == std::any::TypeId::of::<NormaliseStage>());
                    let c = ids
                        .iter()
                        .position(|id| *id == std::any::TypeId::of::<ClampStage>());
                    match (n, c) {
                        (Some(n), Some(c)) if n < c => Ok(()),
                        _ => Err(PipelineError::InvalidState("wrong order".into())),
                    }
                })
                .is_ok()
        );
    }

    #[test]
    fn dynamic_check_fails_with_wrong_ordering() {
        let pipeline = DynamicPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0]))
            .stage(ClampStage)
            .stage(NormaliseStage);

        assert!(
            pipeline
                .check(|ids| {
                    let n = ids
                        .iter()
                        .position(|id| *id == std::any::TypeId::of::<NormaliseStage>());
                    let c = ids
                        .iter()
                        .position(|id| *id == std::any::TypeId::of::<ClampStage>());
                    match (n, c) {
                        (Some(n), Some(c)) if n < c => Ok(()),
                        _ => Err(PipelineError::InvalidState("wrong order".into())),
                    }
                })
                .is_err()
        );
    }

    #[test]
    fn static_check_passes_with_correct_ordering() {
        type StageFn = fn(&mut MlScratchpad) -> Result<(), PipelineError>;

        let mut pipeline = StaticPipeline::<MlScratchpad, 2>::new(MlScratchpad::new(vec![1.0]));
        pipeline.add_stage(normalise).unwrap();
        pipeline.add_stage(clamp).unwrap();

        assert!(
            pipeline
                .check(|fns| {
                    let n = fns
                        .iter()
                        .position(|f| std::ptr::fn_addr_eq(*f, normalise as StageFn));
                    let c = fns
                        .iter()
                        .position(|f| std::ptr::fn_addr_eq(*f, clamp as StageFn));
                    match (n, c) {
                        (Some(n), Some(c)) if n < c => Ok(()),
                        _ => Err(PipelineError::InvalidState("wrong order".into())),
                    }
                })
                .is_ok()
        );
    }
}

mod pool_tests {
    use super::*;

    fn make_pipeline() -> StaticPipeline<MlScratchpad, 2> {
        let mut p = StaticPipeline::new(MlScratchpad::new(vec![1.0, 2.0, 4.0, 8.0]));
        p.add_stage(normalise).unwrap();
        p.add_stage(clamp).unwrap();
        p
    }

    #[test]
    fn pooled_pipeline_produces_correct_results() {
        let pool = PipelinePool::new(2, make_pipeline);
        let mut guard = pool.acquire();
        guard.run().unwrap();
        assert!((guard.context().clamped[3] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn scratchpad_is_reset_between_acquisitions() {
        let pool = PipelinePool::new(1, make_pipeline);
        {
            let mut guard = pool.acquire();
            guard.run().unwrap();
            assert!(guard.context().clamped.iter().any(|x| *x != 0.0));
        }
        let guard = pool.acquire();
        assert!(guard.context().clamped.iter().all(|x| *x == 0.0));
    }

    #[test]
    fn concurrent_acquisitions_produce_independent_results() {
        let pool = Arc::new(PipelinePool::new(4, make_pipeline));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pool = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut guard = pool.acquire();
                    guard.run().unwrap();
                    assert!((guard.context().clamped[3] - 0.9).abs() < 1e-6);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}

mod consistency_tests {
    use super::*;

    #[test]
    fn dynamic_and_static_produce_identical_results() {
        let data = vec![1.0, 2.0, 4.0, 8.0];

        let mut dynamic = DynamicPipeline::new(MlScratchpad::new(data.clone()))
            .stage(NormaliseStage)
            .stage(ClampStage);
        dynamic.run().unwrap();

        let mut static_p = StaticPipeline::<MlScratchpad, 2>::new(MlScratchpad::new(data));
        static_p.add_stage(normalise).unwrap();
        static_p.add_stage(clamp).unwrap();
        static_p.run().unwrap();

        assert_eq!(dynamic.context().clamped, static_p.context().clamped);
    }
}
