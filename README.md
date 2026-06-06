# pipex

A zero-allocation pipeline executor for deterministic workloads in Rust.

Individual pipeline stages transform a shared scratchpad in sequence. The pipeline owns no data and never touches the allocator on the execution path. No scheduler, no async runtime, and no hidden overhead. Designed for domains where performance and low latency are a priority, such as ML inference, robotics, signal processing, real-time control, and embedded systems.
ca
---

## Install

```toml
[dependencies]
pipex = { git = "https://github.com/Ankithboggaram/pipex" }
```

---

## Usage

Define a scratchpad (your shared state), implement stages, run:

```rust
use pipex::dynamic_pipeline::Pipeline;
use pipex::error::PipelineError;
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;

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
}

impl Stage<Buf> for Clamp {
    fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
        ctx.samples.iter_mut().for_each(|x| *x = x.clamp(0.0, 1.0));
        Ok(())
    }
}

let mut pipeline = Pipeline::new().stage(Normalise).stage(Clamp);
let mut ctx = Buf {
    samples: vec![0.5, 2.0, 1.0, 3.0],
};
pipeline.run(&mut ctx).unwrap();
```

For zero-allocation hot paths and concurrent workloads, use `static_pipeline::Pipeline`. Stages are bare function pointers in a fixed-size array; `run` takes `&self`, so a single `Arc<Pipeline>` serves all threads. Pair with `ScratchpadPool` for per-thread buffer reuse:

```rust
use pipex::error::PipelineError;
use pipex::pool::ScratchpadPool;
use pipex::static_pipeline::Pipeline;
use std::sync::Arc;

// Buf is defined above.
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

let mut pipeline = Pipeline::<Buf, 2>::new();
pipeline.add_stage(normalise).unwrap();
pipeline.add_stage(clamp).unwrap();
let pipeline = Arc::new(pipeline);

let pool = Arc::new(ScratchpadPool::new(4, || Buf {
    samples: vec![0.0; 1024],
}));

// Each thread acquires a buffer, runs the pipeline, returns the buffer on drop.
let mut ctx = pool.acquire();
pipeline.run(&mut ctx).unwrap();
```

---

## Wrappers

| Wrapper | What it does |
|---|---|
| `Retry::new(stage, n)` | Retry on failure; restores scratchpad state between attempts |
| `Timed::new(stage, metrics)` | Lock-free nanosecond timing with rolling percentiles |
| `Instrumented::new(stage)` | Emit a [`tracing`](https://docs.rs/tracing) span per execution |
| `Deadline::new(stage, duration)` | Fail if stage exceeds its time budget |

Wrappers compose: `Timed::new(Instrumented::new(stage), metrics)`.

---

## Performance

Measured on Apple Silicon using [divan](https://github.com/nvzqz/divan). All timings are medians. Three stages (normalise, clamp, scale) over varying buffer sizes.

| Data size | Hand-written | Static | Dynamic | Static + Timed |
|---|---|---|---|---|
| 100 | 26 ns | 25 ns | 25 ns | 170 ns |
| 10,000 | 2.3 µs | 2.2 µs | 2.2 µs | 2.3 µs |
| 1,000,000 | 224 µs | 210 µs | 211 µs | 209 µs |

- Static pipeline matches hand-written sequential calls at every data size.
- `Timed` adds ~50 ns per stage (one clock read each side). At small data sizes this dominates; at 10,000+ elements it is unmeasurable against the actual work.
- Pool acquire+run+return (~1.3 µs) vs. allocating a new scratchpad per call (~3.3 µs): ~2.5x faster under load.

---

## License

MIT
