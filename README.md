# pipex: A Generic Pipeline Execution Library in Rust

## About This Project

A personal utility library built while learning Rust. The intent is not publication as a public crate, but to serve as a reusable foundation across my own projects. It will evolve as my Rust knowledge grows.

---

## Core Architecture

`pipex` is built around three concepts:

**Scratchpad**: a reusable memory buffer passed through every stage of the pipeline. Rather than allocating fresh data at each step, the same struct is read from and written to throughout execution. Define your own fields to match your application's needs.

```rust
struct MyScratchpad {
    values: Vec<f32>,
    temp: Vec<f32>,
}

impl Scratchpad for MyScratchpad {
    fn reset(&mut self) { self.values.clear(); self.temp.clear(); }
    fn validate(&self) -> bool { true }
}
```

**Stage**: a single computation step that operates on the scratchpad. Each stage receives a mutable reference, reads what it needs, and writes its output back in place. Stages are composable and independent of each other.

```rust
struct DoubleValues;

impl Stage<MyScratchpad> for DoubleValues {
    fn run(&mut self, ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
        for x in ctx.values.iter_mut() { *x *= 2.0; }
        Ok(())
    }
}
```

**Pipeline**: the executor that runs stages in sequence against the scratchpad. Handles validation before execution and reset between runs. Two implementations are available depending on your performance needs — see Dispatch below.

```rust
let mut pipeline = dynamic_pipeline::Pipeline::new();
pipeline.add_stage(DoubleValues);
pipeline.run(&mut scratchpad)?;
```

---

## Dispatch

`pipex` ships two pipeline implementations:

**`dynamic_pipeline::Pipeline`**: uses `Box<dyn Stage<S>>` for runtime flexibility. Different stage types can be mixed in the same pipeline and stages can be added based on runtime conditions. Has a small vtable lookup overhead on each stage call. Best suited for coarse-grained pipelines where each stage does meaningful work.

**`static_pipeline::Pipeline`**: uses fixed-size arrays of function pointers. No heap allocation after initialisation, no vtable overhead, direct function calls. The pipeline capacity `N` must be known at compile time. Best suited for fixed, performance-critical pipelines.

```rust
// Dynamic: flexible, stages can vary at runtime
let mut pipeline = dynamic_pipeline::Pipeline::new();
pipeline.add_stage(DoubleValues);

// Static: fixed capacity, zero heap allocation
let mut pipeline = static_pipeline::Pipeline::<MyScratchpad, 4>::new();
pipeline.add_stage(double_values)?;
```

---

## Retries

Retry behaviour is opt-in per stage via the `Retry` wrapper. Rather than configuring retries on the pipeline, wrap individual stages that need retry logic. On failure the scratchpad is reset and the stage is retried up to the specified number of times.

```rust
use pipex::retry::Retry;

let mut pipeline = dynamic_pipeline::Pipeline::new();
pipeline.add_stage(Retry::new(DoubleValues, 3));  // retries up to 3 times
pipeline.add_stage(NormaliseValues);               // no retries
pipeline.run(&mut scratchpad)?;
```

---

## Benchmarks

Measured on Apple Silicon using [divan](https://github.com/nvzqz/divan). Scratchpad pre-allocated outside the measured loop to reflect real production usage where the scratchpad is long-lived and reused across runs.

### Scratchpad vs Naive Allocation (single stage)

| Data Size | Dynamic | Static | Naive | Speedup vs Naive |
|---|---|---|---|---|
| 100 elements | 24 ns | 23 ns | 65 ns | ~2.8x |
| 10,000 elements | 1.7 µs | 1.1 µs | 2.5 µs | ~2.3x |
| 1,000,000 elements | 95 µs | 96 µs | 238 µs | ~2.5x |

The scratchpad pattern is consistently **~2.5x faster** than naive per-stage allocation across all data sizes.

### Static vs Dynamic Dispatch (3 stages, data volume)

| Data Size | Dynamic | Static | Static Advantage |
|---|---|---|---|
| 100 elements | 25 ns | 24 ns | ~4% |
| 10,000 elements | 2.1 µs | 2.1 µs | negligible |
| 1,000,000 elements | 204 µs | 205 µs | negligible |

At large data sizes, memory bandwidth dominates and dispatch method becomes irrelevant. Static pipeline shows its advantage at smaller sizes where vtable overhead is proportionally larger.

### Scaling (10,000 elements)

| Stages | Dynamic | Static |
|---|---|---|
| 1 | 1.2 µs | 1.6 µs |
| 5 | 7.7 µs | 7.7 µs |
| 10 | 14.7 µs | 14.9 µs |

Both pipelines scale linearly with stage count. No compounding overhead as stages increase.

### Retry Overhead (10,000 elements, no retries triggered)

| | Median |
|---|---|
| Plain stage | 1.6 µs |
| Retry wrapped | 1.6 µs |

The `Retry` wrapper adds zero measurable overhead when no retries are triggered.

### Mixed Stage Types (dynamic pipeline, 10,000 elements)

| Stages | Median |
|---|---|
| 3 mixed | 2.2 µs |
| 5 mixed | 8.0 µs |
| 10 mixed | 15.3 µs |

Using genuinely different stage types in the same pipeline has no performance penalty compared to same-type stages.

---

## Future Extensions

**Performance**
- [x] Static dispatch pipeline variant: implemented as `static_pipeline::Pipeline`
- [x] Remove retry logic from the core execution loop: retries are now zero-cost when set to 0
- [x] Inline hints on hot path methods
- [x] Skip validation after first successful run
- [x] Benchmarking via `divan` across data sizes, stage counts, and dispatch methods
- [ ] Cache-line alignment hints on scratchpad buffers to reduce CPU cache misses
- [ ] Zero allocation guarantee post-initialisation, with documentation and tests to enforce it
- [ ] SIMD support for numeric data pipelines

**Features**
- [x] Retry mechanism as an opt-in wrapper: implemented as `retry::Retry`
- [ ] Parallel stage execution
- [ ] Arena allocation support
- [ ] Buffer pooling
- [ ] Task graphs

---

## Concepts Encountered While Implementing

- **Structs and ownership**: defining the scratchpad and understanding who owns the data
- **Traits**: defining the `Stage` interface in a pluggable, modular way
- **Lifetimes**: passing references through pipeline stages without unnecessary copies
- **Generics and const generics**: making stages and pipelines work across different types and fixed capacities
- **`Result` and error handling**: deciding what happens when a stage fails
- **Dynamic vs static dispatch**: tradeoffs between `Box<dyn Trait>` and function pointers
- **Modules and `cargo`**: structuring the code as a proper reusable library
- **Doc comments and doctests**: documentation that is automatically tested by `cargo test`
- **Pattern matching**: using `match`, `if let`, and `matches!` for expressive control flow
- **Decorator pattern**: wrapping stages with additional behaviour via `Retry`
- 