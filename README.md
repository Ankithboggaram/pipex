# High-Performance Scratchpad in Rust

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

A scratchpad struct holds all intermediate buffers:

```rust
pub struct Scratchpad {
    pub values: Vec<f32>,
    pub temp: Vec<f32>,
}
```

Computation stages are defined via a trait:

```rust
pub trait Stage<Ctx> {
    fn run(&mut self, ctx: &mut Ctx);
}
```

Each stage takes a mutable reference to the scratchpad, reads what it needs, and writes its output back in place:

```rust
struct MultiplyByTwo;

impl Stage<Scratchpad> for MultiplyByTwo {
    fn run(&mut self, ctx: &mut Scratchpad) {
        for x in ctx.values.iter_mut() {
            *x *= 2.0;
        }
    }
}
```

---

## Dispatch: Static vs Dynamic

**Static dispatch** (`fn run_stage<S: Stage<Ctx>>`)
- Performance: maximum — inlined and monomorphized by the compiler
- Flexibility: fixed at compile time

**Dynamic dispatch** (`Box<dyn Stage<Ctx>>`)
- Performance: slight overhead from vtable lookups
- Flexibility: configurable at runtime

For performance-critical paths, prefer static dispatch. Avoid `Rc<RefCell<T>>`, `Arc<Mutex<T>>`, and `HashMap<TypeId, Box<dyn Any>>` in hot loops — these introduce indirection and cache misses that undermine the scratchpad's purpose.

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

Possible additions as the project matures: SIMD operations, arena allocation, buffer pooling, parallel execution, task graphs.