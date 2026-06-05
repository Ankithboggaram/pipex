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
