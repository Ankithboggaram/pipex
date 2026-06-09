//! Dynamic pipeline: boxed trait objects, runtime composition.
//!
//! Use when stage types are not known at compile time — config-driven pipelines,
//! plugin systems, or test harnesses with mixed stage types.
//!
//! Run with: cargo run --example dynamic_pipeline

use pipexec::dynamic_pipeline::Pipeline;
use pipexec::error::PipelineError;
use pipexec::scratchpad::Scratchpad;
use pipexec::stage::Stage;

struct Buf {
    samples: Vec<f32>,
}

impl Scratchpad for Buf {
    fn reset(&mut self) {
        self.samples.iter_mut().for_each(|x| *x = 0.0);
    }
}

struct Normalise;
struct Clamp;
struct Scale {
    factor: f32,
}

impl Stage<Buf> for Normalise {
    fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
        let max = ctx
            .samples
            .iter()
            .copied()
            .fold(f32::NEG_INFINITY, f32::max);
        if max > 0.0 {
            ctx.samples.iter_mut().for_each(|x| *x /= max);
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Normalise"
    }
}

impl Stage<Buf> for Clamp {
    fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
        ctx.samples.iter_mut().for_each(|x| *x = x.clamp(0.0, 1.0));
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Clamp"
    }
}

impl Stage<Buf> for Scale {
    fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
        ctx.samples.iter_mut().for_each(|x| *x *= self.factor);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Scale"
    }
}

fn main() {
    // Builder style: stages are boxed and stored as trait objects. The pipeline
    // is assembled at runtime; stage types can differ freely.
    let mut pipeline = Pipeline::new()
        .stage(Normalise)
        .stage(Clamp)
        .stage(Scale { factor: 2.0 });

    let mut ctx = Buf {
        samples: vec![0.5, 2.0, 1.0, 3.0],
    };

    pipeline.run(&mut ctx).unwrap();
    println!("{:?}", ctx.samples);
}
