use pipex::dynamic_pipeline::Pipeline as DynamicPipeline;
use pipex::error::PipelineError;
use pipex::retry::Retry;
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::static_pipeline::Pipeline as StaticPipeline;

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
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);
        pipeline.add_stage(ClampStage);

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
    fn returns_validation_error_on_empty_scratchpad() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);
        let mut ctx = MlScratchpad::new(vec![]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn returns_stage_error_on_failure() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(AlwaysFailStage);
        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn stage_error_propagates_message() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);
        let mut ctx = MlScratchpad::new(vec![0.0, 0.0, 0.0]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn can_run_multiple_times() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);

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
        let mut pipeline = StaticPipeline::<MlScratchpad, 2>::new();
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
            Err(PipelineError::StageFailed(_))
        ));
    }

    #[test]
    fn returns_validation_error_on_empty_scratchpad() {
        let mut pipeline = StaticPipeline::<MlScratchpad, 1>::new();
        pipeline.add_stage(normalise).unwrap();
        let mut ctx = MlScratchpad::new(vec![]);
        assert!(matches!(
            pipeline.run(&mut ctx),
            Err(PipelineError::ValidationFailed(_))
        ));
    }
}

mod retry_tests {
    use super::*;

    #[test]
    fn succeeds_on_first_attempt() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(Retry::new(NormaliseStage, 3));

        let mut ctx = MlScratchpad::new(vec![1.0, 2.0, 4.0]);
        assert!(pipeline.run(&mut ctx).is_ok());
    }

    #[test]
    fn exhausts_retries_on_persistent_failure() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(Retry::new(AlwaysFailStage, 2));

        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        let result = pipeline.run(&mut ctx);
        assert!(matches!(result, Err(PipelineError::RetryExhausted { .. })));
    }

    #[test]
    fn resets_scratchpad_between_attempts() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(Retry::new(AlwaysFailStage, 2));

        let mut ctx = MlScratchpad::new(vec![1.0, 2.0]);
        pipeline.run(&mut ctx).ok();

        assert!(ctx.normalised.iter().all(|x| *x == 0.0));
    }
}

mod consistency_tests {
    use super::*;

    #[test]
    fn dynamic_and_static_produce_identical_results() {
        let mut dynamic = DynamicPipeline::new();
        dynamic.add_stage(NormaliseStage);
        dynamic.add_stage(ClampStage);

        let mut static_p = StaticPipeline::<MlScratchpad, 2>::new();
        static_p.add_stage(normalise).unwrap();
        static_p.add_stage(clamp).unwrap();

        let data = vec![1.0, 2.0, 4.0, 8.0];

        let mut dynamic_ctx = MlScratchpad::new(data.clone());
        dynamic.run(&mut dynamic_ctx).unwrap();

        let mut static_ctx = MlScratchpad::new(data);
        static_p.run(&mut static_ctx).unwrap();

        assert_eq!(dynamic_ctx.clamped, static_ctx.clamped);
    }
}
