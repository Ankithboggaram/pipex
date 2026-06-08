//! Pooling: Arc<Pipeline> + ScratchpadPool for concurrent workloads.
//!
//! The pool holds pre-allocated scratchpads. Each thread acquires one,
//! runs the pipeline, and returns it on drop — no allocation per request.
//!
//! Run with: cargo run --example pooling

use std::sync::Arc;

use pipex::error::PipelineError;
use pipex::pool::ScratchpadPool;
use pipex::scratchpad::Scratchpad;
use pipex::static_pipeline::Pipeline;

const BUFFER_SIZE: usize = 1024;

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

fn main() {
    let mut pipeline = Pipeline::<Buf, 2>::new();
    pipeline.add_stage(normalise).unwrap();
    pipeline.add_stage(clamp).unwrap();
    let pipeline = Arc::new(pipeline);

    // Pre-allocate 4 scratchpads. The pool grows beyond capacity under
    // burst load and shrinks back as overflow buffers are dropped.
    let pool = Arc::new(ScratchpadPool::new(4, || Buf {
        samples: vec![0.0; BUFFER_SIZE],
    }));

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let pipeline = Arc::clone(&pipeline);
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || {
                // acquire() checks out a scratchpad; Drop resets it and returns it.
                let mut ctx = pool.acquire();
                ctx.samples[0] = i as f32 * 0.5 + 0.1;
                pipeline.run(&mut ctx).unwrap();
                println!("thread {i}: samples[0] = {:.4}", ctx.samples[0]);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    println!("pool available after all threads: {}", pool.available());
}
