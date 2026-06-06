use divan::{Bencher, black_box};
use pipex::deadline::Deadline;
use pipex::dynamic_pipeline::Pipeline as DynamicPipeline;
use pipex::error::PipelineError;
use pipex::instrument::Instrumented;
use pipex::metrics::Timed;
use pipex::pool::ScratchpadPool;
use pipex::retry::Retry;
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::static_pipeline::Pipeline as StaticPipeline;
use std::time::Duration;

fn main() {
    divan::main();
}

#[derive(Clone)]
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
        let mut pipeline = DynamicPipeline::new().stage(NormaliseStage);
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn static_pipeline(bencher: Bencher, size: usize) {
        let mut pipeline = StaticPipeline::<BenchScratchpad, 1>::new();
        pipeline.add_stage(normalise).unwrap();
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
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
            black_box(pipeline.run(&mut ctx)).unwrap();
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
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }
}

mod data_volume {
    use super::*;
    use stages::*;

    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn dynamic(bencher: Bencher, size: usize) {
        let mut pipeline = DynamicPipeline::new()
            .stage(NormaliseStage)
            .stage(ClampStage)
            .stage(ScaleStage);
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
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
            black_box(pipeline.run(&mut ctx)).unwrap();
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
        let mut pipeline = DynamicPipeline::new().stage(NormaliseStage);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn retry_wrapped_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new().stage(Retry::new(NormaliseStage, 3));
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
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
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }
}

mod deadline_overhead {
    use super::*;
    use stages::*;

    #[divan::bench]
    fn plain_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new().stage(NormaliseStage);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn deadline_wrapped_stage(bencher: Bencher) {
        let mut pipeline =
            DynamicPipeline::new().stage(Deadline::new(NormaliseStage, Duration::from_secs(1)));
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }
}

mod pool_overhead {
    use super::*;
    use stages::*;

    fn make_static_pipeline() -> StaticPipeline<BenchScratchpad, 1> {
        let mut p = StaticPipeline::new();
        p.add_stage(normalise).unwrap();
        p
    }

    /// Allocates a fresh scratchpad (3 x Vec<f32> of 10k elements) on every call.
    /// This is the realistic baseline for a server that creates a scratchpad per request.
    #[divan::bench]
    fn new_scratchpad_per_call(bencher: Bencher) {
        let pipeline = make_static_pipeline();

        bencher.bench_local(|| {
            let mut ctx = BenchScratchpad::new(10_000);
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    /// Acquires a pre-built scratchpad from the pool and returns it on drop.
    /// No allocation on the hot path; Vec buffers are reused.
    #[divan::bench]
    fn pool_acquire_run_return(bencher: Bencher) {
        let pipeline = make_static_pipeline();
        let pool = ScratchpadPool::new(4, || BenchScratchpad::new(10_000));

        bencher.bench_local(|| {
            let mut ctx = pool.acquire();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }
}

mod instrumentation_overhead {
    use super::*;
    use stages::*;

    #[divan::bench]
    fn plain_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new().stage(NormaliseStage);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn timed_stage(bencher: Bencher) {
        let (normalise_timed, _normalise_metrics) = Timed::new(NormaliseStage);
        let mut pipeline = DynamicPipeline::new().stage(normalise_timed);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn instrumented_stage(bencher: Bencher) {
        let mut pipeline = DynamicPipeline::new().stage(Instrumented::new(NormaliseStage));
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    #[divan::bench]
    fn timed_and_instrumented_stage(bencher: Bencher) {
        let (normalise_timed, _normalise_metrics) = Timed::new(Instrumented::new(NormaliseStage));
        let mut pipeline = DynamicPipeline::new().stage(normalise_timed);
        let mut ctx = BenchScratchpad::new(10_000);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }
}

// The headline comparison: bare sequential calls vs. pipex abstractions.
// If pipex static is within noise of hand_written, the zero-overhead claim holds.
mod sequential_comparison {
    use super::*;
    use stages::*;

    // Bare sequential function calls — no loop, no abstraction, no indirection.
    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn hand_written(bencher: Bencher, size: usize) {
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(normalise(&mut ctx)).unwrap();
            black_box(clamp(&mut ctx)).unwrap();
            black_box(scale(&mut ctx)).unwrap();
        });
    }

    // Manual function pointer array loop — what you'd write without pipex.
    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn fn_pointer_loop(bencher: Bencher, size: usize) {
        type StageFn = fn(&mut BenchScratchpad) -> Result<(), PipelineError>;
        let stages: [StageFn; 3] = [normalise, clamp, scale];
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            for stage in &stages {
                black_box(stage(&mut ctx)).unwrap();
            }
        });
    }

    // pipex static pipeline — function pointers, zero allocation.
    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn pipex_static(bencher: Bencher, size: usize) {
        let mut pipeline = StaticPipeline::<BenchScratchpad, 3>::new();
        pipeline.add_stage(normalise).unwrap();
        pipeline.add_stage(clamp).unwrap();
        pipeline.add_stage(scale).unwrap();
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    // pipex dynamic pipeline — boxed trait objects, runtime composition.
    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn pipex_dynamic(bencher: Bencher, size: usize) {
        let mut pipeline = DynamicPipeline::new()
            .stage(NormaliseStage)
            .stage(ClampStage)
            .stage(ScaleStage);
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }

    // pipex static + per-stage timing — cost of full observability on the hot path.
    #[divan::bench(args = [100, 10_000, 1_000_000])]
    fn pipex_static_timed(bencher: Bencher, size: usize) {
        let (normalise_timed, _normalise_metrics) = Timed::new(NormaliseStage);
        let (clamp_timed, _clamp_metrics) = Timed::new(ClampStage);
        let (scale_timed, _scale_metrics) = Timed::new(ScaleStage);
        let mut pipeline = DynamicPipeline::new()
            .stage(normalise_timed)
            .stage(clamp_timed)
            .stage(scale_timed);
        let mut ctx = BenchScratchpad::new(size);

        bencher.bench_local(|| {
            ctx.reset();
            black_box(pipeline.run(&mut ctx)).unwrap();
        });
    }
}
