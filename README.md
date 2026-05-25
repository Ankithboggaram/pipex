# pipex — A Generic Pipeline Execution Library in Rust

## About This Project

A personal utility library built while learning Rust. The intent is not publication as a public crate, but to serve as a reusable foundation across my own projects. It will evolve as my Rust knowledge grows.

---

## What is a Scratchpad?

A scratchpad is a single, reusable memory structure shared across multiple stages of a computation pipeline. Rather than allocating fresh data structures at each stage, the same buffers are read from and written to throughout execution. This reduces heap pressure, improves cache locality, and produces more predictable runtime performance.

It is a common pattern in game engines, compilers, ML inference runtimes, and signal processing systems — anywhere a fixed pipeline runs repeatedly at high frequency.

---

## The Problem With Naive Pipelines

A straightforward pipeline implementation allocates at every step:

```rust
fn stage1() -> Vec<f32> { ... }
fn stage2(input: Vec<f32>) -> Vec<f32> { ... }
fn stage3(input: Vec<f32>) -> Vec<f32> { ... }
```

This produces repeated heap allocations, unnecessary copies, and allocator overhead — costs that compound quickly in real-time or high-throughput contexts.

---

## Core Architecture

`pipex` is built around three concepts:

**Scratchpad** — a trait you implement on your own struct. Defines `reset()` and `validate()`, called by the pipeline at the right moments. Your struct holds whatever buffers your application needs.

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

**Stage** — a trait you implement for each computation step. Receives a mutable reference to the scratchpad, reads what it needs, and writes its output back in place.

```rust
struct DoubleValues;

impl Stage<MyScratchpad> for DoubleValues {
    fn run(&mut self, ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
        for x in ctx.values.iter_mut() { *x *= 2.0; }
        Ok(())
    }
}
```

**Pipeline** — holds a sequence of stages and runs them in order against the scratchpad. Handles validation, reset between runs, and retries on failure.

```rust
let mut pipeline = Pipeline::new().with_retries(3);
pipeline.add_stage(DoubleValues);
pipeline.run(&mut scratchpad)?;
```

---

## Dispatch

`pipex` uses dynamic dispatch to store stages in the pipeline:

```rust
stages: Vec<Box<dyn Stage<S>>>
```

This allows the pipeline to hold stages of different types in the same collection at runtime. The tradeoff is a small overhead from vtable lookups on each stage call, which is acceptable for a pipeline execution library where flexibility is more important than micro-optimising individual calls.

For the same reason, avoid using `pipex` in tight inner loops where every nanosecond counts. It is best suited for coarse-grained pipelines where each stage does a meaningful amount of work.

---

## Concepts Encountered While Implementing

- **Structs and ownership** — defining the scratchpad and understanding who owns the data
- **Traits** — defining the `Stage` interface in a pluggable, modular way
- **Lifetimes** — passing references through pipeline stages without unnecessary copies
- **Generics** — making stages work across different scratchpad types
- **`Result` and error handling** — deciding what happens when a stage fails
- **Modules and `cargo`** — structuring the code as a proper reusable library

---

## Future Extensions

**Performance**
- Static dispatch pipeline variant to eliminate vtable overhead and enable compiler inlining
- Remove retry logic from the core execution loop — retries should be an opt-in wrapper, not baked into the hot path
- Cache-line alignment hints on scratchpad buffers to reduce CPU cache misses
- Zero allocation guarantee post-initialisation, with documentation and tests to enforce it
- Benchmarking via `criterion` comparing `pipex` against naive allocation-per-stage pipelines

**Features**
- Parallel stage execution
- Arena allocation support
- Buffer pooling
- Task graphs