# pipex

A high-performance, zero-allocation pipeline execution library for Rust. Stages operate on a shared pre-allocated scratchpad buffer, reading inputs and writing outputs in place. No heap allocation occurs on the execution hot path.

---

## Add to your project

```toml
[dependencies]
pipex = { git = "https://github.com/Ankithboggaram/pipex" }
```

---

## Quick start

Three steps: define a scratchpad, implement your stages, run the pipeline.

```rust
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::dynamic_pipeline::Pipeline;
use pipex::error::PipelineError;

// 1. Define your scratchpad — the shared buffer stages read from and write to.
struct MyScratchpad {
    input: Vec<f32>,
    output: Vec<f32>,
}

impl Scratchpad for MyScratchpad {
    fn reset(&mut self) { self.output.iter_mut().for_each(|x| *x = 0.0); }
    fn validate(&self) -> bool { !self.input.is_empty() }
}

// 2. Implement your stages.
struct NormaliseStage;

impl Stage<MyScratchpad> for NormaliseStage {
    fn run(&mut self, ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
        let max = ctx.input.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        ctx.output.iter_mut().zip(ctx.input.iter()).for_each(|(o, i)| *o = i / max);
        Ok(())
    }
}

// 3. Build and run.
let mut pipeline = Pipeline::new();
pipeline.add_stage(NormaliseStage);

let mut ctx = MyScratchpad { input: vec![1.0, 2.0, 4.0], output: vec![0.0; 3] };
pipeline.run(&mut ctx).unwrap();
```

---

## Choosing a pipeline type

**Dynamic** — use when stage types differ or the pipeline is configured at runtime.

```rust
use pipex::dynamic_pipeline::Pipeline;

let mut pipeline = Pipeline::new();
pipeline.add_stage(NormaliseStage);
pipeline.add_stage(ClampStage);
```

**Static** — use when all stages are known at compile time. Zero heap allocation after setup, no vtable overhead. Capacity is fixed at `N`.

```rust
use pipex::static_pipeline::Pipeline;

let mut pipeline = Pipeline::<MyScratchpad, 2>::new();
pipeline.add_stage(normalise)?;
pipeline.add_stage(clamp)?;
```

---

## Retry

Wrap any stage in `Retry` to re-run it on failure. The scratchpad is reset between attempts.

```rust
use pipex::retry::Retry;

pipeline.add_stage(Retry::new(NormaliseStage, 3)); // up to 3 retries
```

---

## Metrics

Wrap any stage in `Timed` to collect per-stage execution latency. Metrics are lock-free and include rolling window percentiles (p50, p95, p99, p99.9).

```rust
use pipex::metrics::{StageMetrics, Timed};
use std::sync::Arc;

let metrics = StageMetrics::new("normalise");
pipeline.add_stage(Timed::new(NormaliseStage, Arc::clone(&metrics)));

let snapshot = metrics.snapshot();
println!("p99: {}ns  p999: {}ns  errors: {}", snapshot.p99_ns, snapshot.p999_ns, snapshot.error_count);
```

---

## Tracing

Wrap any stage in `Instrumented` to emit a [`tracing`](https://docs.rs/tracing) span on every execution.

```rust
use pipex::instrument::Instrumented;

pipeline.add_stage(Instrumented::new(NormaliseStage, "normalise"));
```

---

## Composing wrappers

All wrappers compose cleanly.

```rust
pipeline.add_stage(Timed::new(Instrumented::new(NormaliseStage, "normalise"), Arc::clone(&metrics)));
```

---

## Performance

Measured on Apple Silicon using [divan](https://github.com/nvzqz/divan). Scratchpad pre-allocated outside the measured loop.

**Scratchpad vs naive allocation (single stage)**

| Data size | Dynamic | Static | Naive | vs Naive |
|---|---|---|---|---|
| 100 | 24 ns | 23 ns | 65 ns | ~2.8x |
| 10,000 | 1.7 µs | 1.1 µs | 2.5 µs | ~2.3x |
| 1,000,000 | 95 µs | 96 µs | 238 µs | ~2.5x |

**Scaling (10,000 elements)**

| Stages | Dynamic | Static |
|---|---|---|
| 1 | 1.2 µs | 1.6 µs |
| 5 | 7.7 µs | 7.7 µs |
| 10 | 14.7 µs | 14.9 µs |

Both pipelines scale linearly. At large data sizes dispatch method is irrelevant — memory bandwidth dominates. Static pipeline advantage is most visible at small data sizes where vtable overhead is proportionally larger.

**Wrapper overhead**: `Instrumented` is zero-cost when no tracing subscriber is configured. `Timed` adds ~70ns per stage call for atomic writes and clock reads.

**Retry overhead**: zero measurable overhead when no retries are triggered.

**Mixed stage types**: no performance penalty versus same-type stages.

**Zero allocation guarantee**: verified by test — neither pipeline allocates during `run()` on the success path.

---

## Roadmap

**Performance**
- [x] Static dispatch pipeline
- [x] Zero-cost retry path when retries disabled
- [x] Inline hints on hot path methods
- [x] Skip validation after first successful run
- [x] Zero allocation guarantee: verified by automated test
- [x] Cache-line alignment hints on scratchpad buffers
- [ ] Type chaining for full compiler inlining across stage boundaries
- [ ] SIMD support for numeric pipelines

**Features**
- [x] Per-stage retry via `retry::Retry`
- [x] Per-stage timing metrics via `metrics::Timed`
- [x] Per-stage tracing spans via `instrument::Instrumented`
- [ ] Parallel stage execution
- [ ] Arena allocation
- [ ] Buffer pooling
- [ ] Task graphs
