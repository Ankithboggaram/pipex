use divan::{Bencher, black_box};
use pipex::dynamic_pipeline::Pipeline as DynamicPipeline;
use pipex::error::PipelineError;
use pipex::instrument::Instrumented;
use pipex::metrics::{StageMetrics, Timed};
use pipex::retry::Retry;
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::static_pipeline::Pipeline as StaticPipeline;
use std::sync::Arc;

fn main() {
    divan::main();
}

struct BenchScratchpad {
    input: Vec<f32>,
    output: Vec<f32>,
    temp: Vec<f32>,
}

impl BenchScratchpad {
    fn new(size: usize) -> Self {
        Self {
            input: (0..size).map(|i| i as f32).collect(),
            output: vec![0.0; size],
            temp: vec![0.0; size],
        }
    }
}

impl Scratchpad for BenchScratchpad {
    fn reset(&mut self) {
        self.input.iter_mut().for_each(|x| *x = 0.0);
        self.output.iter_mut().for_each(|x| *x = 0.0);
        self.temp.iter_mut().for_each(|x| *x = 0.0);
    }

    fn validate(&self) -> bool {
        !self.input.is_empty()
    }
}

mod stages {
    use super::*;

    pub struct NormaliseStage;
    impl Stage<BenchScratchpad> for NormaliseStage {
        fn run(&mut self, ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
            let max = ctx.input.iter().copied().fold(f32::NEG_INFINITY, f32::max);
            if max == 0.0 {
                return Ok(());
            }
            ctx.output
                .iter_mut()
                .zip(ctx.input.iter())
                .for_each(|(o, i)| *o = i / max);
            Ok(())
        }
    }

    pub struct ClampStage;
    impl Stage<BenchScratchpad> for ClampStage {
        fn run(&mut self, ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
            ctx.output.iter_mut().for_each(|x| *x = x.clamp(0.1, 0.9));
            Ok(())
        }
    }

    pub struct ScaleStage;
    impl Stage<BenchScratchpad> for ScaleStage {
        fn run(&mut self, ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
            ctx.output.iter_mut().for_each(|x| *x *= 2.0);
            Ok(())
        }
    }

    pub struct SumReduceStage;
    impl Stage<BenchScratchpad> for SumReduceStage {
        fn run(&mut self, ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
            let sum = ctx.output.iter().sum::<f32>();
            ctx.temp.iter_mut().for_each(|x| *x = sum);
            Ok(())
        }
    }

    pub struct DeltaStage;
    impl Stage<BenchScratchpad> for DeltaStage {
        fn run(&mut self, ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
            for i in 1..ctx.output.len() {
                ctx.temp[i] = ctx.output[i] - ctx.output[i - 1];
            }
            Ok(())
        }
    }

    pub fn normalise(ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
        let max = ctx.input.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        if max == 0.0 {
            return Ok(());
        }
        ctx.output
            .iter_mut()
            .zip(ctx.input.iter())
            .for_each(|(o, i)| *o = i / max);
        Ok(())
    }

    pub fn clamp(ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
        ctx.output.iter_mut().for_each(|x| *x = x.clamp(0.1, 0.9));
        Ok(())
    }

    pub fn scale(ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
        ctx.output.iter_mut().for_each(|x| *x *= 2.0);
        Ok(())
    }

    pub fn sum_reduce(ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
        let sum = ctx.output.iter().sum::<f32>();
        ctx.temp.iter_mut().for_each(|x| *x = sum);
        Ok(())
    }

    pub fn delta(ctx: &mut BenchScratchpad) -> Result<(), PipelineError> {
        for i in 1..ctx.output.len() {
            ctx.temp[i] = ctx.output[i] - ctx.output[i - 1];
        }
        Ok(())
    }

    pub fn naive_pipeline(data: &[f32]) -> Vec<f32> {
        let max = data.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let normalised: Vec<f32> = data.iter().map(|x| x / max).collect();
        let clamped: Vec<f32> = normalised.iter().map(|x| x.clamp(0.1, 0.9)).collect();
        clamped.iter().map(|x| x * 2.0).collect()
    }
}

mod single_stage {
    use super::*;
    use stages::*;

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn dynamic(bencher: Bencher, size: usize) {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn static_pipeline(bencher: Bencher, size: usize) {
        let mut pipeline = StaticPipeline::<BenchScratchpad, 1>::new();
        pipeline.add_stage(normalise).unwrap();
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn naive(bencher: Bencher, size: usize) {
        let data: Vec<f32> = (0..size).map(|i| i as f32).collect();

        bencher.bench_local(|| {
            black_box(naive_pipeline(&data));
        });
    }
}

mod scaling {
    use super::*;
    use stages::*;

    type StageFn = fn(&mut BenchScratchpad) -> Result<(), PipelineError>;

    fn add_stages_dynamic(pipeline: &mut DynamicPipeline<BenchScratchpad>, count: usize) {
        for i in 0..count {
            match i % 5 {
                0 => pipeline.add_stage(NormaliseStage),
                1 => pipeline.add_stage(ClampStage),
                2 => pipeline.add_stage(ScaleStage),
                3 => pipeline.add_stage(SumReduceStage),
                _ => pipeline.add_stage(DeltaStage),
            }
        }
    }

    #[divan::bench(args = [1, 5, 10])]
    fn dynamic(bencher: Bencher, count: usize) {
        let mut pipeline = DynamicPipeline::new();
        add_stages_dynamic(&mut pipeline, count);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench(args = [1, 5, 10])]
    fn static_pipeline(bencher: Bencher, count: usize) {
        let static_fns: [StageFn; 10] = [
            normalise, clamp, scale, sum_reduce, delta, normalise, clamp, scale, sum_reduce, delta,
        ];
        let mut pipeline = StaticPipeline::<BenchScratchpad, 10>::new();
        for stage_fn in static_fns.iter().take(count) {
            pipeline.add_stage(*stage_fn).unwrap();
        }
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }
}

mod data_volume {
    use super::*;
    use stages::*;

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn dynamic(bencher: Bencher, size: usize) {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);
        pipeline.add_stage(ClampStage);
        pipeline.add_stage(ScaleStage);
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn static_pipeline(bencher: Bencher, size: usize) {
        let mut pipeline = StaticPipeline::<BenchScratchpad, 3>::new();
        pipeline.add_stage(normalise).unwrap();
        pipeline.add_stage(clamp).unwrap();
        pipeline.add_stage(scale).unwrap();
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn naive(bencher: Bencher, size: usize) {
        let data: Vec<f32> = (0..size).map(|i| i as f32).collect();

        bencher.bench_local(|| {
            black_box(naive_pipeline(&data));
        });
    }
}

mod retry_overhead {
    use super::*;
    use stages::*;

    #[divan::bench]
    fn plain_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn retry_wrapped_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(Retry::new(NormaliseStage, 3));
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }
}

mod mixed_stages {
    use super::*;
    use stages::*;

    #[divan::bench(args = [3, 5, 10])]
    fn dynamic(bencher: Bencher, count: usize) {
        let mut pipeline = DynamicPipeline::new();
        for i in 0..count {
            match i % 5 {
                0 => pipeline.add_stage(NormaliseStage),
                1 => pipeline.add_stage(ClampStage),
                2 => pipeline.add_stage(ScaleStage),
                3 => pipeline.add_stage(SumReduceStage),
                _ => pipeline.add_stage(DeltaStage),
            }
        }
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }
}

mod instrumentation_overhead {
    use super::*;
    use stages::*;

    #[divan::bench]
    fn plain_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(NormaliseStage);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn timed_stage(bencher: Bencher) {
        let metrics = StageMetrics::new("normalise");
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(Timed::new(NormaliseStage, Arc::clone(&metrics)));
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn instrumented_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(Instrumented::new(NormaliseStage, "normalise"));
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn timed_and_instrumented_stage(bencher: Bencher) {
        let metrics = StageMetrics::new("normalise");
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(Timed::new(
            Instrumented::new(NormaliseStage, "normalise"),
            Arc::clone(&metrics),
        ));
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            pipeline.run(black_box(&mut ctx)).unwrap();
        });
    }
}
