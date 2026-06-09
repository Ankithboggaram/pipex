//! Thread-safe pool of pre-allocated scratchpads for concurrent workloads.
//!
//! Share one pipeline via [`Arc`] across all threads; each
//! thread acquires a [`ScratchpadGuard`] from the pool, runs the pipeline, and
//! on drop the scratchpad is reset and returned. Scratchpads beyond the pool
//! capacity are dropped rather than cached, bounding memory use.
//!
//! For async callers that need to hold a guard across `.await` points, use
//! [`ScratchpadPool::acquire_owned`], which returns an [`OwnedScratchpadGuard`]
//! that holds an [`Arc`] clone of the pool and is freely `Send`.
//!
//! With pipelines decoupled from their scratchpad, the typical concurrent
//! pattern is to share a single [`static_pipeline::Pipeline`][crate::static_pipeline::Pipeline] via [`Arc`] and
//! pool only the scratchpads, one per concurrent caller.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use pipex::pool::ScratchpadPool;
//! use pipex::static_pipeline::Pipeline;
//! use pipex::scratchpad::Scratchpad;
//! use pipex::error::PipelineError;
//!
//! struct Buf { value: f32 }
//!
//! impl Scratchpad for Buf {
//!     fn reset(&mut self) { self.value = 0.0; }
//! }
//!
//! fn double(ctx: &mut Buf) -> Result<(), PipelineError> {
//!     ctx.value *= 2.0;
//!     Ok(())
//! }
//!
//! let mut pipeline = Pipeline::<Buf, 1>::new();
//! pipeline.add_stage(double).unwrap();
//! let pipeline = Arc::new(pipeline);
//!
//! let pool = Arc::new(ScratchpadPool::new(4, || Buf { value: 0.0 }));
//!
//! let mut ctx = pool.acquire();
//! ctx.value = 3.0;
//! pipeline.run(&mut ctx).unwrap();
//! assert_eq!(ctx.value, 6.0);
//! // ctx drops here → scratchpad reset → returned to pool
//! ```

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::scratchpad::Scratchpad;

/// A thread-safe pool of scratchpads for concurrent pipeline workloads.
///
/// Share one pipeline via [`Arc`] across all threads; each thread acquires its
/// own [`ScratchpadGuard`] from this pool. On drop, [`Scratchpad::reset`] is
/// called and the scratchpad is returned. Scratchpads beyond
/// [`capacity`][ScratchpadPool::capacity] are dropped rather than cached,
/// bounding memory use.
pub struct ScratchpadPool<S: Scratchpad + Send> {
    slots: Mutex<Vec<S>>,
    factory: Box<dyn Fn() -> S + Send + Sync>,
    capacity: usize,
}

impl<S: Scratchpad + Send> ScratchpadPool<S> {
    /// Creates a pool pre-populated with `capacity` scratchpads built by `factory`.
    ///
    /// `factory` is also called whenever all scratchpads are simultaneously in use.
    pub fn new(capacity: usize, factory: impl Fn() -> S + Send + Sync + 'static) -> Self {
        let factory: Box<dyn Fn() -> S + Send + Sync> = Box::new(factory);
        let slots = (0..capacity).map(|_| factory()).collect();
        Self {
            slots: Mutex::new(slots),
            factory,
            capacity,
        }
    }

    /// Checks out a scratchpad for synchronous use.
    ///
    /// The returned [`ScratchpadGuard`] borrows `self` for its lifetime. For
    /// async callers that need to hold the guard inside a spawned task, use
    /// [`acquire_owned`][Self::acquire_owned] instead.
    ///
    /// If the pool is empty, a new scratchpad is built by the factory. The
    /// scratchpad is reset and returned to the pool (up to capacity) when the
    /// guard drops.
    #[must_use]
    pub fn acquire(&self) -> ScratchpadGuard<'_, S> {
        let scratchpad = self.slots.lock().pop().unwrap_or_else(|| (self.factory)());
        ScratchpadGuard {
            scratchpad: Some(scratchpad),
            pool: self,
        }
    }

    /// Checks out a scratchpad for use inside async tasks.
    ///
    /// The returned [`OwnedScratchpadGuard`] holds an [`Arc`] clone of the pool
    /// rather than a borrowed reference, so it carries no lifetime annotation
    /// and is `'static`. This satisfies the `Future: 'static` requirement of
    /// `tokio::spawn` and async trait methods in frameworks like tonic, where
    /// a lifetime-bound [`ScratchpadGuard`] would not compile.
    ///
    /// Use [`acquire`][Self::acquire] at synchronous call sites; it avoids the
    /// [`Arc`] clone overhead.
    ///
    /// If the pool is empty, a new scratchpad is built by the factory. The
    /// scratchpad is reset and returned to the pool (up to capacity) when the
    /// guard drops.
    #[must_use]
    pub fn acquire_owned(self: &Arc<Self>) -> OwnedScratchpadGuard<S> {
        let scratchpad = self.slots.lock().pop().unwrap_or_else(|| (self.factory)());
        OwnedScratchpadGuard {
            scratchpad: Some(scratchpad),
            pool: Arc::clone(self),
        }
    }

    /// Returns the number of scratchpads currently idle in the pool.
    #[must_use]
    pub fn available(&self) -> usize {
        self.slots.lock().len()
    }

    /// Returns the maximum number of scratchpads the pool will hold at rest.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// RAII guard that holds a scratchpad checked out from a [`ScratchpadPool`].
///
/// Derefs to `S`, giving direct access to the scratchpad. On drop,
/// [`Scratchpad::reset`] is called and the scratchpad is returned to the pool
/// (provided the pool is not already at capacity).
pub struct ScratchpadGuard<'a, S: Scratchpad + Send> {
    scratchpad: Option<S>,
    pool: &'a ScratchpadPool<S>,
}

impl<S: Scratchpad + Send> Drop for ScratchpadGuard<'_, S> {
    fn drop(&mut self) {
        if let Some(mut scratchpad) = self.scratchpad.take() {
            scratchpad.reset();
            let mut slots = self.pool.slots.lock();
            if slots.len() < self.pool.capacity {
                slots.push(scratchpad);
            }
        }
    }
}

impl<S: Scratchpad + Send> Deref for ScratchpadGuard<'_, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.scratchpad.as_ref().expect("guard used after drop")
    }
}

impl<S: Scratchpad + Send> DerefMut for ScratchpadGuard<'_, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scratchpad.as_mut().expect("guard used after drop")
    }
}

/// RAII guard returned by [`ScratchpadPool::acquire_owned`].
///
/// Identical to [`ScratchpadGuard`] in behaviour — resets and returns the
/// scratchpad on drop — but holds an [`Arc`] clone of the pool instead of a
/// borrowed reference. This makes the guard `'static`, which is required for
/// futures spawned with `tokio::spawn` or held in async trait methods.
pub struct OwnedScratchpadGuard<S: Scratchpad + Send> {
    scratchpad: Option<S>,
    pool: Arc<ScratchpadPool<S>>,
}

impl<S: Scratchpad + Send> Drop for OwnedScratchpadGuard<S> {
    fn drop(&mut self) {
        if let Some(mut scratchpad) = self.scratchpad.take() {
            scratchpad.reset();
            let mut slots = self.pool.slots.lock();
            if slots.len() < self.pool.capacity {
                slots.push(scratchpad);
            }
        }
    }
}

impl<S: Scratchpad + Send> Deref for OwnedScratchpadGuard<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.scratchpad.as_ref().expect("guard used after drop")
    }
}

impl<S: Scratchpad + Send> DerefMut for OwnedScratchpadGuard<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scratchpad.as_mut().expect("guard used after drop")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PipelineError;
    use crate::static_pipeline::Pipeline;

    struct Buf {
        value: f32,
    }

    impl Buf {
        fn new(value: f32) -> Self {
            Self { value }
        }
    }

    impl Scratchpad for Buf {
        fn reset(&mut self) {
            self.value = 0.0;
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn double(ctx: &mut Buf) -> Result<(), PipelineError> {
        ctx.value *= 2.0;
        Ok(())
    }

    fn make_pipeline() -> Pipeline<Buf, 1> {
        let mut p = Pipeline::new();
        p.add_stage(double).unwrap();
        p
    }

    #[test]
    fn pool_pre_populates_to_capacity() {
        let pool: ScratchpadPool<Buf> = ScratchpadPool::new(4, || Buf::new(0.0));
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.capacity(), 4);
    }

    #[test]
    fn acquire_reduces_available_count() {
        let pool: ScratchpadPool<Buf> = ScratchpadPool::new(2, || Buf::new(0.0));
        let _g1 = pool.acquire();
        assert_eq!(pool.available(), 1);
        let _g2 = pool.acquire();
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn drop_returns_scratchpad_to_pool() {
        let pool: ScratchpadPool<Buf> = ScratchpadPool::new(1, || Buf::new(0.0));
        {
            let _guard = pool.acquire();
            assert_eq!(pool.available(), 0);
        }
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn pipeline_runs_correctly_with_pooled_scratchpad() {
        let pipeline = make_pipeline();
        let pool: ScratchpadPool<Buf> = ScratchpadPool::new(1, || Buf::new(0.0));
        let mut ctx = pool.acquire();
        ctx.value = 3.0;
        pipeline.run(&mut ctx).unwrap();
        assert_eq!(ctx.value, 6.0);
    }

    #[test]
    fn scratchpad_is_reset_on_return() {
        let pool: ScratchpadPool<Buf> = ScratchpadPool::new(1, || Buf::new(0.0));
        {
            let mut ctx = pool.acquire();
            ctx.value = 99.0;
        }
        let ctx = pool.acquire();
        assert_eq!(ctx.value, 0.0);
    }

    #[test]
    fn factory_creates_scratchpad_when_pool_exhausted() {
        let pool: ScratchpadPool<Buf> = ScratchpadPool::new(1, || Buf::new(0.0));
        let _g1 = pool.acquire();
        let mut g2 = pool.acquire();
        g2.value = 2.0;
        assert_eq!(g2.value, 2.0);
    }

    #[test]
    fn overflow_scratchpad_is_dropped_not_returned() {
        let pool: ScratchpadPool<Buf> = ScratchpadPool::new(1, || Buf::new(0.0));
        let g1 = pool.acquire();
        let g2 = pool.acquire();
        drop(g1);
        drop(g2);
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn pool_is_usable_across_threads() {
        use std::sync::Arc;

        let pipeline = Arc::new(make_pipeline());
        let pool = Arc::new(ScratchpadPool::new(4, || Buf::new(0.0)));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pipeline = Arc::clone(&pipeline);
                let pool = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut ctx = pool.acquire();
                    ctx.value = 2.0;
                    pipeline.run(&mut ctx).unwrap();
                    assert_eq!(ctx.value, 4.0);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn acquire_owned_checks_out_scratchpad() {
        use std::sync::Arc;

        let pool = Arc::new(ScratchpadPool::new(2, || Buf::new(0.0)));
        assert_eq!(pool.available(), 2);
        let _guard = pool.acquire_owned();
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn acquire_owned_drop_returns_scratchpad() {
        use std::sync::Arc;

        let pool = Arc::new(ScratchpadPool::new(1, || Buf::new(0.0)));
        {
            let _guard = pool.acquire_owned();
            assert_eq!(pool.available(), 0);
        }
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn acquire_owned_resets_scratchpad_on_return() {
        use std::sync::Arc;

        let pool = Arc::new(ScratchpadPool::new(1, || Buf::new(0.0)));
        {
            let mut ctx = pool.acquire_owned();
            ctx.value = 42.0;
        }
        let ctx = pool.acquire_owned();
        assert_eq!(ctx.value, 0.0);
    }

    #[test]
    fn acquire_owned_guard_is_send() {
        use std::sync::Arc;

        fn assert_send<T: Send>(_: T) {}

        let pool = Arc::new(ScratchpadPool::new(1, || Buf::new(0.0)));
        let guard = pool.acquire_owned();
        assert_send(guard);
    }

    #[test]
    fn acquire_owned_usable_across_threads() {
        use std::sync::Arc;

        let pipeline = Arc::new(make_pipeline());
        let pool = Arc::new(ScratchpadPool::new(4, || Buf::new(0.0)));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pipeline = Arc::clone(&pipeline);
                let pool = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut ctx = pool.acquire_owned();
                    ctx.value = 2.0;
                    pipeline.run(&mut ctx).unwrap();
                    assert_eq!(ctx.value, 4.0);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
