//! Tuple chain: zero-allocation composition with per-stage wrappers.
//!
//! A tuple of stages is itself a Stage. Wrappers (Timed, Retry, Deadline,
//! Instrumented) are ordinary tuple elements — no special pipeline support needed.
//!
//! Run with: cargo run --example tuple_chain

use pipexec::error::PipelineError;
use pipexec::metrics::Timed;
use pipexec::retry::Retry;
use pipexec::scratchpad::Scratchpad;
use pipexec::stage::Stage;

#[derive(Clone)]
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
struct Scale;

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
        ctx.samples.iter_mut().for_each(|x| *x *= 2.0);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Scale"
    }
}

fn main() {
    // Timed::new returns the wrapper and its metrics handle together.
    let (timed_clamp, clamp_metrics) = Timed::new(Clamp);

    // Retry::new wraps a stage; the scratchpad is restored on each failed attempt.
    // Buf: Clone is required for Retry.
    let retried_scale = Retry::new(Scale, 3);

    // The tuple is itself a Stage — no pipeline type required.
    // All stage state is stored inline; no heap allocation, no dynamic dispatch.
    let mut pipeline = (Normalise, timed_clamp, retried_scale);

    let mut ctx = Buf {
        samples: vec![0.5, 2.0, 1.0, 3.0],
    };

    for _ in 0..10 {
        pipeline.run(&mut ctx).unwrap();
        ctx.samples = vec![0.5, 2.0, 1.0, 3.0];
    }

    let snap = clamp_metrics.snapshot();
    println!("Clamp ran {} times", snap.count);
    println!("Clamp p99: {}ns", snap.p99_ns);
}
