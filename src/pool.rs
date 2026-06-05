//! Thread-safe pool of pipelines for concurrent workloads.
//!
//! Each pipeline owns its scratchpad, so concurrent callers need separate
//! pipeline instances rather than sharing one. `PipelinePool` manages a
//! fixed-capacity stock of pre-built pipelines: callers [`acquire`] a
//! [`PoolGuard`], use the pipeline, and on drop the scratchpad is reset
//! and the pipeline is returned for the next caller.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use pipex::pool::PipelinePool;
//! use pipex::static_pipeline::Pipeline;
//! use pipex::scratchpad::Scratchpad;
//! use pipex::error::PipelineError;
//!
//! struct Buf { value: f32 }
//!
//! impl Scratchpad for Buf {
//!     fn reset(&mut self) { self.value = 0.0; }
//!     fn validate(&self) -> bool { true }
//! }
//!
//! fn double(ctx: &mut Buf) -> Result<(), PipelineError> {
//!     ctx.value *= 2.0;
//!     Ok(())
//! }
//!
//! let pool: Arc<PipelinePool<Pipeline<Buf, 1>>> = Arc::new(PipelinePool::new(4, || {
//!     let mut p = Pipeline::new(Buf { value: 0.0 });
//!     p.add_stage(double).unwrap();
//!     p
//! }));
//!
//! let mut guard = pool.acquire();
//! guard.context_mut().value = 3.0;
//! guard.run().unwrap();
//! assert_eq!(guard.context().value, 6.0);
//! // pipeline is reset and returned when guard is dropped
//! ```
//!
//! [`acquire`]: PipelinePool::acquire

use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

/// Implemented by pipeline types that can be held in a [`PipelinePool`].
///
/// Both [`static_pipeline::Pipeline`][crate::static_pipeline::Pipeline] and
/// [`dynamic_pipeline::Pipeline`][crate::dynamic_pipeline::Pipeline] implement this.
pub trait PoolablePipeline {
    /// Resets the pipeline's scratchpad and clears cached validation state,
    /// making it safe to re-issue to the next caller.
    fn reset_for_reuse(&mut self);
}

/// A thread-safe pool of pre-built pipelines for concurrent workloads.
///
/// Call [`acquire`][PipelinePool::acquire] to borrow a pipeline. The returned
/// [`PoolGuard`] resets the pipeline and returns it to the pool on drop.
/// When all pooled pipelines are simultaneously in use the factory closure
/// creates a new one on demand; pipelines beyond [`capacity`][PipelinePool::capacity]
/// are dropped on return rather than cached, bounding memory use.
pub struct PipelinePool<P: PoolablePipeline + Send> {
    pipelines: Mutex<Vec<P>>,
    factory: Box<dyn Fn() -> P + Send + Sync>,
    capacity: usize,
}

impl<P: PoolablePipeline + Send> PipelinePool<P> {
    /// Creates a pool pre-populated with `capacity` pipelines built by `factory`.
    ///
    /// `factory` is also called whenever all pipelines are simultaneously in use.
    pub fn new(capacity: usize, factory: impl Fn() -> P + Send + Sync + 'static) -> Self {
        let factory: Box<dyn Fn() -> P + Send + Sync> = Box::new(factory);
        let pipelines = (0..capacity).map(|_| factory()).collect();
        Self {
            pipelines: Mutex::new(pipelines),
            factory,
            capacity,
        }
    }

    /// Borrows a pipeline from the pool.
    ///
    /// If the pool is empty a new pipeline is created via the factory. The
    /// pipeline is reset and returned to the pool (up to capacity) when the
    /// guard is dropped.
    ///
    /// # Panics
    ///
    /// Panics if the pool's internal mutex is poisoned (i.e. a thread panicked
    /// while holding it).
    #[must_use]
    pub fn acquire(&self) -> PoolGuard<'_, P> {
        let pipeline = self
            .pipelines
            .lock()
            .expect("pool mutex poisoned")
            .pop()
            .unwrap_or_else(|| (self.factory)());
        PoolGuard {
            pipeline: Some(pipeline),
            pool: self,
        }
    }

    /// Returns the number of pipelines currently idle in the pool.
    ///
    /// # Panics
    ///
    /// Panics if the pool's internal mutex is poisoned.
    #[must_use]
    pub fn available(&self) -> usize {
        self.pipelines.lock().expect("pool mutex poisoned").len()
    }

    /// Returns the maximum number of pipelines the pool will hold at rest.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// RAII guard that holds a pipeline checked out from a [`PipelinePool`].
///
/// Derefs to `P`, so all pipeline methods are accessible directly. On drop,
/// [`PoolablePipeline::reset_for_reuse`] is called and the pipeline is
/// returned to the pool (provided the pool is not already at capacity).
pub struct PoolGuard<'a, P: PoolablePipeline + Send> {
    pipeline: Option<P>,
    pool: &'a PipelinePool<P>,
}

impl<P: PoolablePipeline + Send> Drop for PoolGuard<'_, P> {
    fn drop(&mut self) {
        if let Some(mut pipeline) = self.pipeline.take() {
            pipeline.reset_for_reuse();
            if let Ok(mut guard) = self.pool.pipelines.lock() {
                if guard.len() < self.pool.capacity {
                    guard.push(pipeline);
                }
            }
        }
    }
}

impl<P: PoolablePipeline + Send> Deref for PoolGuard<'_, P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        self.pipeline.as_ref().expect("guard used after drop")
    }
}

impl<P: PoolablePipeline + Send> DerefMut for PoolGuard<'_, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.pipeline.as_mut().expect("guard used after drop")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PipelineError;
    use crate::scratchpad::Scratchpad;
    use crate::static_pipeline::Pipeline;

    struct Buf {
        value: f32,
        valid: bool,
    }

    impl Buf {
        fn new(value: f32) -> Self {
            Self { value, valid: true }
        }
    }

    impl Scratchpad for Buf {
        fn reset(&mut self) {
            self.value = 0.0;
        }
        fn validate(&self) -> bool {
            self.valid
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn double(ctx: &mut Buf) -> Result<(), PipelineError> {
        ctx.value *= 2.0;
        Ok(())
    }

    fn make_pipeline() -> Pipeline<Buf, 1> {
        let mut p = Pipeline::new(Buf::new(0.0));
        p.add_stage(double).unwrap();
        p
    }

    #[test]
    fn pool_pre_populates_to_capacity() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(4, make_pipeline);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.capacity(), 4);
    }

    #[test]
    fn acquire_reduces_available_count() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(2, make_pipeline);
        let _g1 = pool.acquire();
        assert_eq!(pool.available(), 1);
        let _g2 = pool.acquire();
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn drop_returns_pipeline_to_pool() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(1, make_pipeline);
        {
            let _guard = pool.acquire();
            assert_eq!(pool.available(), 0);
        }
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn acquired_pipeline_runs_correctly() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(1, make_pipeline);
        let mut guard = pool.acquire();
        guard.context_mut().value = 3.0;
        guard.run().unwrap();
        assert_eq!(guard.context().value, 6.0);
    }

    #[test]
    fn scratchpad_is_reset_on_return() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(1, make_pipeline);
        {
            let mut guard = pool.acquire();
            guard.context_mut().value = 99.0;
            guard.run().unwrap();
        }
        let guard = pool.acquire();
        assert_eq!(guard.context().value, 0.0);
    }

    #[test]
    fn validation_reruns_after_return() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(1, make_pipeline);
        {
            let mut guard = pool.acquire();
            guard.context_mut().value = 2.0;
            guard.run().unwrap();
        }
        let mut guard = pool.acquire();
        guard.context_mut().valid = false;
        assert!(matches!(
            guard.run(),
            Err(PipelineError::ValidationFailed(_))
        ));
    }

    #[test]
    fn factory_creates_pipeline_when_pool_exhausted() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(1, make_pipeline);
        let _g1 = pool.acquire();
        let mut g2 = pool.acquire();
        g2.context_mut().value = 2.0;
        g2.run().unwrap();
        assert_eq!(g2.context().value, 4.0);
    }

    #[test]
    fn overflow_pipeline_is_dropped_not_returned() {
        let pool: PipelinePool<Pipeline<Buf, 1>> = PipelinePool::new(1, make_pipeline);
        let g1 = pool.acquire();
        let g2 = pool.acquire(); // overflow — comes from factory
        drop(g1); // returned (available = 1, at capacity)
        drop(g2); // dropped — would exceed capacity
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn pool_is_usable_across_threads() {
        use std::sync::Arc;

        let pool = Arc::new(PipelinePool::new(4, make_pipeline));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pool = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut guard = pool.acquire();
                    guard.context_mut().value = 2.0;
                    guard.run().unwrap();
                    assert_eq!(guard.context().value, 4.0);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
