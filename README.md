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

## Future Extensions

**Performance**
- [x] Static dispatch pipeline variant: implemented as `static_pipeline::Pipeline`
- [x] Remove retry logic from the core execution loop: retries are now zero-cost when set to 0
- [ ] Type chaining static pipeline for full compiler inlining across stage boundaries
- [ ] Cache-line alignment hints on scratchpad buffers to reduce CPU cache misses
- [ ] Zero allocation guarantee post-initialisation, with documentation and tests to enforce it
- [ ] Benchmarking via `criterion` comparing `pipex` against naive allocation-per-stage pipelines
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
