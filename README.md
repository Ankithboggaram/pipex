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

let mut pipeline = Pipeline::new().stage(Double);
let mut ctx = Buf { value: 3.0 };
pipeline.run(&mut ctx).unwrap();
assert_eq!(ctx.value, 6.0);
```

Two pipeline variants:

- **Dynamic** (`dynamic_pipeline::Pipeline`): stages as `Box<dyn Stage<S>>`, configured at runtime.
- **Static** (`static_pipeline::Pipeline<S, N>`): stages as function pointers in a fixed array, zero allocation after setup. Takes `&self` on `run` — share one instance across threads via `Arc`.

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

The static pipeline is stateless after setup, so a single `Arc<Pipeline>` can be shared across all threads. Pool only the scratchpads — one per concurrent caller.

The factory closure is called once per slot at startup (and on demand if all scratchpads are in use). `acquire()` checks out a scratchpad; dropping the guard resets it and returns it to the pool.

```rust
use std::sync::Arc;
use pipex::pool::ScratchpadPool;
use pipex::static_pipeline::Pipeline;

// One pipeline shared across all threads.
let mut pipeline = Pipeline::<Buf, 1>::new();
pipeline.add_stage(double).unwrap();
let pipeline = Arc::new(pipeline);

// Pool of scratchpads — one per concurrent caller.
let pool = Arc::new(ScratchpadPool::new(4, || Buf { value: 0.0 }));

// Spawn 8 threads, each borrowing a scratchpad from the pool.
for _ in 0..8 {
    let pipeline = Arc::clone(&pipeline);
    let pool = Arc::clone(&pool);
    std::thread::spawn(move || {
        let mut ctx = pool.acquire();  // borrows a scratchpad
        ctx.value = 3.0;              // write input
        pipeline.run(&mut ctx).unwrap();
        assert_eq!(ctx.value, 6.0);
        // ctx drops here → scratchpad reset → returned to pool
    });
}
```

---

## Performance

Measured on Apple Silicon using [divan](https://github.com/nvzqz/divan). All timings are medians.

**Reused scratchpad vs. allocating on every call** (single stage, varying buffer size)

| Data size | Dynamic | Static | Naive | vs Naive |
|---|---|---|---|---|
| 100 | 17 ns | 17 ns | 64 ns | ~3.9x |
| 10,000 | 1.6 µs | 1.1 µs | 2.4 µs | ~2.2x |
| 1,000,000 | 97 µs | 97 µs | 237 µs | ~2.4x |

**Pipeline cost scales linearly with stage count** (10,000 elements)

| Stages | Dynamic | Static |
|---|---|---|
| 1 | 1.6 µs | 1.4 µs |
| 5 | 7.8 µs | 7.8 µs |
| 10 | 15.0 µs | 15.0 µs |

- `Timed` adds ~75 ns per stage. `Instrumented`, `Deadline`, and `Retry` (no retries triggered) are zero-cost.
- Pool acquire+run+return (~1.3 µs) is ~2.5x faster than allocating a new scratchpad per request (~3.3 µs).

---

## Roadmap

- [ ] Type chaining for full compiler inlining across stage boundaries
- [ ] Parallel stage execution
- [ ] Arena allocation
