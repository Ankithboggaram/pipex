//! Static pipeline: function pointers in a fixed-size array, shared via Arc.
//!
//! Run with: cargo run --example static_pipeline

use std::sync::Arc;

use pipexec::error::PipelineError;
use pipexec::scratchpad::Scratchpad;
use pipexec::static_pipeline::Pipeline;

struct Buf {
    samples: Vec<f32>,
}

impl Scratchpad for Buf {
    fn reset(&mut self) {
        self.samples.iter_mut().for_each(|x| *x = 0.0);
    }
}

fn normalise(ctx: &mut Buf) -> Result<(), PipelineError> {
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

fn clamp(ctx: &mut Buf) -> Result<(), PipelineError> {
    ctx.samples.iter_mut().for_each(|x| *x = x.clamp(0.0, 1.0));
    Ok(())
}

fn scale(ctx: &mut Buf) -> Result<(), PipelineError> {
    ctx.samples.iter_mut().for_each(|x| *x *= 2.0);
    Ok(())
}

fn main() {
    let mut pipeline = Pipeline::<Buf, 3>::new();
    pipeline.add_stage(normalise).unwrap();
    pipeline.add_stage(clamp).unwrap();
    pipeline.add_stage(scale).unwrap();

    // run takes &self — a single Arc<Pipeline> can be shared across threads
    // with no Mutex required.
    let pipeline = Arc::new(pipeline);

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let pipeline = Arc::clone(&pipeline);
            std::thread::spawn(move || {
                let mut ctx = Buf {
                    samples: vec![0.5, 2.0, 1.0, 3.0],
                };
                pipeline.run(&mut ctx).unwrap();
                println!("thread {i}: {:?}", ctx.samples);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}
