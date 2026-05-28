# pipex

A high-performance, zero-allocation pipeline execution library for Rust. Stages operate on a shared pre-allocated scratchpad buffer, reading inputs and writing outputs in place. No heap allocation occurs on the execution hot path.

---

## Usage

Define a scratchpad, implement stages, build a pipeline:

```rust
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::dynamic_pipeline::Pipeline;
use pipex::error::PipelineError;

struct MyScratchpad {
    input: Vec<f32>,
    output: Vec<f32>,
}

impl Scratchpad for MyScratchpad {
    fn reset(&mut self) { self.output.iter_mut().for_each(|x| *x = 0.0); }
    fn validate(&self) -> bool { !self.input.is_empty() }
}

struct NormaliseStage;

impl Stage<MyScratchpad> for NormaliseStage {
    fn run(&mut self, ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
        let max = ctx.input.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        ctx.output.iter_mut().zip(ctx.input.iter()).for_each(|(o, i)| *o = i / max);
        Ok(())
    }
}

let mut pipeline = Pipeline::new();
pipeline.add_stage(NormaliseStage);

let mut ctx = MyScratchpad { input: vec![1.0, 2.0, 4.0], output: vec![0.0; 3] };
pipeline.run(&mut ctx).unwrap();
```

---

## Pipelines

**`dynamic_pipeline::Pipeline`**: stores stages as `Box<dyn Stage<S>>`. Supports mixed stage types and runtime configuration. Small vtable overhead per stage call.

**`static_pipeline::Pipeline`**: stores stages as fixed-size arrays of function pointers. Zero heap allocation after initialisation, no vtable overhead. Capacity `N` fixed at compile time.

```rust
// Dynamic
let mut pipeline = dynamic_pipeline::Pipeline::new();
pipeline.add_stage(NormaliseStage);

// Static
let mut pipeline = static_pipeline::Pipeline::<MyScratchpad, 4>::new();
pipeline.add_stage(normalise_fn)?;
```

---

## Retries

Per-stage retry logic via the `Retry` wrapper. The scratchpad is reset between attempts.

```rust
use pipex::retry::Retry;

pipeline.add_stage(Retry::new(NormaliseStage, 3));
pipeline.add_stage(ClampStage);
```

---

## Observability

**Timing metrics** via `Timed` — lock-free per-stage latency tracking with rolling window percentiles. Zero locking, atomic operations only.

```rust
use pipex::metrics::{StageMetrics, Timed};
use std::sync::Arc;

let metrics = StageMetrics::new("normalise");
pipeline.add_stage(Timed::new(NormaliseStage, Arc::clone(&metrics)));

let snapshot = metrics.snapshot();
println!("p99: {}ns  p999: {}ns  errors: {}", snapshot.p99_ns, snapshot.p999_ns, snapshot.error_count);
```

**Tracing spans** via `Instrumented` — emits structured spans on every stage execution. Integrates with the `tracing` ecosystem. Zero overhead when no subscriber is configured.

```rust
use pipex::instrument::Instrumented;

pipeline.add_stage(Instrumented::new(NormaliseStage, "normalise"));
```

Both wrappers compose cleanly:

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

**Zero allocation guarantee**: verified by test — neither pipeline allocates during `run()` after initialisation.

---

## Roadmap

**Performance**
- [x] Static dispatch pipeline
- [x] Zero-cost retry path when retries disabled
- [x] Inline hints on hot path methods
- [x] Skip validation after first successful run
- [x] Zero allocation guarantee: verified by automated test
- [ ] Cache-line alignment hints on scratchpad buffers
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
