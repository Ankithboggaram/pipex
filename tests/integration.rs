use pipex::deadline::Deadline;
use pipex::dynamic_pipeline::Pipeline as DynamicPipeline;
use pipex::error::PipelineError;
use pipex::metrics::{PipelineMetrics, Timed};
use pipex::pool::ScratchpadPool;
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
        let mut pipeline = DynamicPipeline::new()
            .stage(NormaliseStage)
            .stage(ClampStage);
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0, 8.0]);
        pipeline.run(&mut ctx).unwrap();

        assert!((ctx.clamped[0] - 0.125).abs() < 1e-6);
        assert!((ctx.clamped[1] - 0.25).abs() < 1e-6);
        assert!((ctx.clamped[2] - 0.5).abs() < 1e-6);
        assert!((ctx.clamped[3] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn returns_empty_pipeline_error() {
        let mut pipeline: DynamicPipeline<MlScratchpad> = DynamicPipeline::new();
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::EmptyPipeline)
        ));
    }

    #[test]
    fn returns_stage_error_on_failure() {
        let mut pipeline = DynamicPipeline::new().stage(AlwaysFailStage);
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn stage_error_propagates_message() {
        let mut pipeline = DynamicPipeline::new().stage(NormaliseStage);
        let mut ctx = MlScratchpad::new(vec![0.0, 0.0, 0.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn can_run_multiple_times() {
        let mut pipeline = DynamicPipeline::new().stage(NormaliseStage);
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        pipeline.run(&mut ctx).unwrap();
        let first_result = ctx.normalised.clone();

        ctx.raw = vec![2.0, 4.0, 8.0];
        ctx.reset();
        pipeline.run(&mut ctx).unwrap();

        assert_eq!(ctx.normalised, first_result);
    }
}

mod static_pipeline_tests {
    use super::*;

    #[test]
    fn runs_stages_in_order() {
        let mut pipeline = StaticPipeline::<MlScratchpad, 2>::new();
        pipeline.add_stage(normalise).unwrap();
        pipeline.add_stage(clamp).unwrap();
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0, 8.0]);
        pipeline.run(&mut ctx).unwrap();

        assert!((ctx.clamped[0] - 0.125).abs() < 1e-6);
        assert!((ctx.clamped[1] - 0.25).abs() < 1e-6);
        assert!((ctx.clamped[2] - 0.5).abs() < 1e-6);
        assert!((ctx.clamped[3] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn returns_empty_pipeline_error() {
        let pipeline = StaticPipeline::<MlScratchpad, 2>::new();
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::EmptyPipeline)
        ));
    }

    #[test]
    fn returns_error_when_over_capacity() {
        let mut pipeline = StaticPipeline::<MlScratchpad, 1>::new();
        pipeline.add_stage(normalise).unwrap();
        assert!(matches!(
            pipeline.add_stage(clamp),
            Err(PipelineError::FullPipeline)
        ));
    }
}

mod retry_tests {
    use super::*;

    #[test]
    fn succeeds_on_first_attempt() {
        let mut pipeline = DynamicPipeline::new().stage(Retry::new(NormaliseStage, 3));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        assert!(pipeline.run(&mut ctx).is_ok());
    }

    #[test]
    fn exhausts_retries_on_persistent_failure() {
        let mut pipeline = DynamicPipeline::new().stage(Retry::new(AlwaysFailStage, 2));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::RetryExhausted { .. })
        ));
    }

    #[test]
    fn resets_scratchpad_between_attempts() {
        let mut pipeline = DynamicPipeline::new().stage(Retry::new(AlwaysFailStage, 2));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        pipeline.run(&mut ctx).ok();
        assert!(ctx.normalised.iter().all(|x| *x == 0.0));
    }
}

mod deadline_tests {
    use super::*;

    #[test]
    fn fast_stage_completes_within_budget() {
        let mut pipeline =
            DynamicPipeline::new().stage(Deadline::new(NormaliseStage, Duration::from_secs(1)));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        assert!(pipeline.run(&mut ctx).is_ok());
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

        let mut pipeline =
            DynamicPipeline::new().stage(Deadline::new(SlowStage, Duration::from_millis(1)));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        match pipeline.run(&mut ctx) {
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
        let mut pipeline =
            DynamicPipeline::new().stage(Deadline::new(AlwaysFailStage, Duration::from_nanos(1)));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn deadline_composes_with_retry() {
        let mut pipeline = DynamicPipeline::new().stage(Retry::new(
            Deadline::new(NormaliseStage, Duration::from_secs(1)),
            2,
        ));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        assert!(pipeline.run(&mut ctx).is_ok());
    }
}

mod pipeline_metrics_tests {
    use super::*;

    #[test]
    fn tracks_execution_count_across_stages() {
        let mut pm = PipelineMetrics::new();
        let mut pipeline = DynamicPipeline::new()
            .stage(Timed::new(NormaliseStage, pm.track("normalise")))
            .stage(Timed::new(ClampStage, pm.track("clamp")));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        pipeline.run(&mut ctx).unwrap();

        let snapshot = pm.snapshot();
        assert_eq!(snapshot.stages.len(), 2);
        assert_eq!(snapshot.total_count(), 2);
        assert_eq!(snapshot.stages[0].count, 1);
        assert_eq!(snapshot.stages[1].count, 1);
    }

    #[test]
    fn snapshot_identifies_slowest_stage() {
        let mut pm = PipelineMetrics::new();
        let mut pipeline = DynamicPipeline::new()
            .stage(Timed::new(NormaliseStage, pm.track("normalise")))
            .stage(Timed::new(ClampStage, pm.track("clamp")));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        for _ in 0..10 {
            ctx.reset();
            pipeline.run(&mut ctx).unwrap();
        }

        assert!(pm.snapshot().slowest_stage().is_some());
    }

    #[test]
    fn error_stages_filters_correctly() {
        let mut pm = PipelineMetrics::new();
        let normalise_m = pm.track("normalise");
        let fail_m = pm.track("always_fail");

        let mut pipeline =
            DynamicPipeline::new().stage(Timed::new(NormaliseStage, Arc::clone(&normalise_m)));
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        pipeline.run(&mut ctx).unwrap();

        let mut fail_stage = Timed::new(AlwaysFailStage, Arc::clone(&fail_m));
        let mut ctx2 = MlScratchpad::new(vec![1.0]);
        fail_stage.run(&mut ctx2).ok();

        let snapshot = pm.snapshot();
        let errored: Vec<_> = snapshot.error_stages().collect();
        assert_eq!(errored.len(), 1);
        assert_eq!(errored[0].label, "always_fail");
    }
}

mod ordering_tests {
    use super::*;

    #[test]
    fn static_check_passes_with_correct_ordering() {
        type StageFn = fn(&mut MlScratchpad) -> Result<(), PipelineError>;

        let mut pipeline = StaticPipeline::<MlScratchpad, 2>::new();
        pipeline.add_stage(normalise).unwrap();
        pipeline.add_stage(clamp).unwrap();

        assert!(
            pipeline
                .check(|fns| {
                    let n = fns.iter().position(|f| {
                        f.is_some_and(|f| std::ptr::fn_addr_eq(f, normalise as StageFn))
                    });
                    let c = fns
                        .iter()
                        .position(|f| f.is_some_and(|f| std::ptr::fn_addr_eq(f, clamp as StageFn)));
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

    fn make_pool() -> ScratchpadPool<MlScratchpad> {
        ScratchpadPool::new(2, || MlScratchpad::new(vec![1.0, 2.0, 4.0, 8.0]))
    }

    fn make_pipeline() -> StaticPipeline<MlScratchpad, 2> {
        let mut p = StaticPipeline::new();
        p.add_stage(normalise).unwrap();
        p.add_stage(clamp).unwrap();
        p
    }

    #[test]
    fn pooled_scratchpad_produces_correct_results() {
        let pipeline = make_pipeline();
        let pool = make_pool();
        let mut ctx = pool.acquire();
        pipeline.run(&mut ctx).unwrap();
        assert!((ctx.clamped[3] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn scratchpad_is_reset_between_acquisitions() {
        let pipeline = make_pipeline();
        let pool = make_pool();
        {
            let mut ctx = pool.acquire();
            pipeline.run(&mut ctx).unwrap();
            assert!(ctx.clamped.iter().any(|x| *x != 0.0));
        }
        let ctx = pool.acquire();
        assert!(ctx.clamped.iter().all(|x| *x == 0.0));
    }

    #[test]
    fn concurrent_acquisitions_produce_independent_results() {
        let pipeline = Arc::new(make_pipeline());
        let pool = Arc::new(make_pool());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pipeline = Arc::clone(&pipeline);
                let pool = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut ctx = pool.acquire();
                    pipeline.run(&mut ctx).unwrap();
                    assert!((ctx.clamped[3] - 0.9).abs() < 1e-6);
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

        let mut dynamic = DynamicPipeline::new()
            .stage(NormaliseStage)
            .stage(ClampStage);
        let mut dctx = MlScratchpad::new(data.clone());
        dynamic.run(&mut dctx).unwrap();

        let mut static_p = StaticPipeline::<MlScratchpad, 2>::new();
        static_p.add_stage(normalise).unwrap();
        static_p.add_stage(clamp).unwrap();
        let mut sctx = MlScratchpad::new(data);
        static_p.run(&mut sctx).unwrap();

        assert_eq!(dctx.clamped, sctx.clamped);
    }
}
