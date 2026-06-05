# pipex

A high-performance, zero-allocation pipeline execution library for Rust. Stages operate on a shared pre-allocated scratchpad buffer; no heap allocation occurs on the execution hot path.

---

## Add to your project

```toml
[dependencies]
pipex = { git = "https://github.com/Ankithboggaram/pipex" }
```

---

## Quick start

Define a scratchpad, implement stages, run.

```rust
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::dynamic_pipeline::Pipeline;
use pipex::error::PipelineError;

struct Buf {
    value: f32,
}

impl Scratchpad for Buf {
    fn reset(&mut self) {
        self.value = 0.0;
    }
    fn validate(&self) -> bool {
        true
    }
}

struct Double;

impl Stage<Buf> for Double {
    fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
        ctx.value *= 2.0;
        Ok(())
    }
}

let mut pipeline = Pipeline::new(Buf { value: 3.0 }).stage(Double);
pipeline.run().unwrap();
assert_eq!(pipeline.context().value, 6.0);
```

Two pipeline variants:

- **Dynamic** (`dynamic_pipeline::Pipeline`): stages as `Box<dyn Stage<S>>`, configured at runtime.
- **Static** (`static_pipeline::Pipeline<S, N>`): stages as function pointers in a fixed array, zero allocation after setup.

---

## Wrappers

| Wrapper | What it does |
|---|---|
| `Retry::new(stage, n)` | Re-run on failure, resetting the scratchpad between attempts |
| `Timed::new(stage, metrics)` | Lock-free nanosecond timing with rolling window percentiles |
| `Instrumented::new(stage, name)` | Emit a [`tracing`](https://docs.rs/tracing) span on every execution |
| `Deadline::new(stage, duration)` | Return `DeadlineExceeded` if the stage exceeds its time budget |

All wrappers compose: `Timed::new(Instrumented::new(stage, "name"), metrics)`.

Use `PipelineMetrics` to aggregate snapshots across all stages in one call:

```rust
let mut pm = PipelineMetrics::new();
let mut pipeline = Pipeline::new(ctx)
    .stage(Timed::new(StageA, pm.track("a")))
    .stage(Timed::new(StageB, pm.track("b")));

pipeline.run().unwrap();
let snapshot = pm.snapshot();
```

---

## Pooling

Each pipeline owns its scratchpad, so concurrent callers each need their own instance. `PipelinePool` keeps a fixed stock of pre-built pipelines with no allocation on the hot path.

The factory closure is called once per slot at startup (and on demand if all pipelines are in use). `acquire()` checks out a pipeline; dropping the guard resets the scratchpad and returns it.

```rust
use std::sync::Arc;
use pipex::pool::PipelinePool;
use pipex::static_pipeline::Pipeline;

// Build a pool of 4 pipelines, each wired with the Double stage.
let pool = Arc::new(PipelinePool::new(4, || {
    let mut p = Pipeline::new(Buf { value: 0.0 });
    p.add_stage(double).unwrap();
    p
}));

// Spawn 8 threads, each borrowing a pipeline from the shared pool.
for _ in 0..8 {
    let pool = Arc::clone(&pool);
    std::thread::spawn(move || {
        let mut guard = pool.acquire();    // borrows a pipeline
        guard.context_mut().value = 3.0;  // write input
        guard.run().unwrap();              // execute stages
        assert_eq!(guard.context().value, 6.0);
        // guard drops here → scratchpad reset → pipeline returned
    });
}
```

---

## Performance

Measured on Apple Silicon using [divan](https://github.com/nvzqz/divan). All timings are medians.

**Reused scratchpad vs. allocating on every call** (single stage, varying buffer size)

| Data size | Dynamic | Static | Naive | vs Naive |
|---|---|---|---|---|
| 100 | 16 ns | 16 ns | 66 ns | ~4.1x |
| 10,000 | 1.6 µs | 1.1 µs | 2.5 µs | ~2.4x |
| 1,000,000 | 97 µs | 95 µs | 237 µs | ~2.5x |

**Pipeline cost scales linearly with stage count** (10,000 elements)

| Stages | Dynamic | Static |
|---|---|---|
| 1 | 1.4 µs | 1.6 µs |
| 5 | 8.0 µs | 7.8 µs |
| 10 | 15.6 µs | 14.8 µs |

- `Timed` adds ~80 ns per stage. `Instrumented`, `Deadline`, and `Retry` (no retries triggered) are zero-cost.
- Pool acquire+run+return (~1.9 µs) is ~1.7x faster than creating a new pipeline per request (~3.2 µs).

---

## Roadmap

- [ ] Type chaining for full compiler inlining across stage boundaries
- [ ] Parallel stage execution
- [ ] Arena allocation
- [ ] Task graphs
