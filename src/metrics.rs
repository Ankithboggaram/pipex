//! Lock-free per-stage timing metrics with rolling-window percentiles.
//!
//! Wrap any stage in [`Timed`] to record nanosecond execution latency with no
//! locking. Use [`PipelineMetrics`] to aggregate metrics from multiple stages
//! into a single [`PipelineSnapshot`] for dashboards and alerting.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

const WINDOW_SIZE: usize = 1024;

/// A point-in-time snapshot of metrics for a single stage.
#[derive(Debug, Clone)]
pub struct StageSnapshot {
    /// Stage identifier.
    pub label: String,
    /// Total execution count.
    pub count: u64,
    /// Number of executions that returned an error.
    pub error_count: u64,
    /// Error rate as a fraction of total executions (0.0..=1.0).
    pub error_rate: f64,
    /// Duration of the most recent execution, in nanoseconds.
    pub last_ns: u64,
    /// Minimum recorded execution duration, in nanoseconds.
    pub min_ns: u64,
    /// Maximum recorded execution duration, in nanoseconds.
    pub max_ns: u64,
    /// Mean execution duration over all recorded samples, in nanoseconds.
    pub mean_ns: u64,
    /// 50th-percentile duration from the rolling window, in nanoseconds.
    pub p50_ns: u64,
    /// 95th-percentile duration from the rolling window, in nanoseconds.
    pub p95_ns: u64,
    /// 99th-percentile duration from the rolling window, in nanoseconds.
    pub p99_ns: u64,
    /// 99.9th-percentile duration from the rolling window, in nanoseconds.
    pub p999_ns: u64,
}

impl std::fmt::Display for StageSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = if self.error_count == 0 { "OK" } else { "ERR" };
        writeln!(f, "[{}] {}", status, self.label)?;
        writeln!(f, "  Count:              {}", self.count)?;
        writeln!(f, "  Last:               {}ns", self.last_ns)?;
        writeln!(f, "  Min:                {}ns", self.min_ns)?;
        writeln!(f, "  Max:                {}ns", self.max_ns)?;
        writeln!(f, "  Mean:               {}ns", self.mean_ns)?;
        writeln!(f, "  50th Percentile:    {}ns", self.p50_ns)?;
        writeln!(f, "  95th Percentile:    {}ns", self.p95_ns)?;
        writeln!(f, "  99th Percentile:    {}ns", self.p99_ns)?;
        writeln!(f, "  99.9th Percentile:  {}ns", self.p999_ns)?;
        write!(
            f,
            "  Errors:             {} ({:.2}%)",
            self.error_count,
            self.error_rate * 100.0
        )
    }
}

impl StageSnapshot {
    /// Returns a compact single-line summary suitable for multi-stage dashboards.
    #[must_use]
    pub fn summary(&self) -> String {
        let status = if self.error_count == 0 { "OK" } else { "ERR" };
        format!(
            "[{}] {:<20}  p99={:<10}  p999={:<10}  errors={}",
            status,
            self.label,
            format!("{}ns", self.p99_ns),
            format!("{}ns", self.p999_ns),
            self.error_count,
        )
    }
}

/// Lock-free per-stage metrics collector with rolling window percentiles.
///
/// Uses a fixed-size ring buffer of the last 1024 samples for percentile
/// computation. All counters use atomic operations with no locking.
///
/// Aligned to 64 bytes (one cache line) to prevent false sharing when
/// multiple stages each hold their own `StageMetrics`.
#[derive(Debug)]
#[repr(align(64))]
pub struct StageMetrics {
    /// Stage label for identification.
    pub label: String,
    count: AtomicU64,
    error_count: AtomicU64,
    total_ns: AtomicU64,
    min_ns: AtomicU64,
    max_ns: AtomicU64,
    last_ns: AtomicU64,
    window: [AtomicU64; WINDOW_SIZE],
    window_pos: AtomicUsize,
}

impl StageMetrics {
    /// Creates a new `StageMetrics` for the given label.
    #[must_use]
    pub fn new(label: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            label: label.into(),
            count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            min_ns: AtomicU64::new(u64::MAX),
            max_ns: AtomicU64::new(0),
            last_ns: AtomicU64::new(0),
            window: std::array::from_fn(|_| AtomicU64::new(0)),
            window_pos: AtomicUsize::new(0),
        })
    }

    /// Records a single execution.
    pub fn record(&self, duration_ns: u64, failed: bool) {
        self.count.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.error_count.fetch_add(1, Ordering::Relaxed);
        }
        self.total_ns.fetch_add(duration_ns, Ordering::Relaxed);
        self.last_ns.store(duration_ns, Ordering::Relaxed);
        fetch_min(&self.min_ns, duration_ns);
        fetch_max(&self.max_ns, duration_ns);

        let pos = self.window_pos.fetch_add(1, Ordering::Relaxed) % WINDOW_SIZE;
        self.window[pos].store(duration_ns, Ordering::Relaxed);
    }

    /// Returns a point-in-time snapshot of all current metrics.
    #[must_use]
    pub fn snapshot(&self) -> StageSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let error_count = self.error_count.load(Ordering::Relaxed);
        let total_ns = self.total_ns.load(Ordering::Relaxed);
        let min_ns = {
            let min = self.min_ns.load(Ordering::Relaxed);
            if min == u64::MAX { 0 } else { min }
        };
        let max_ns = self.max_ns.load(Ordering::Relaxed);
        let mean_ns = total_ns.checked_div(count).unwrap_or(0);
        let error_rate = if count == 0 {
            0.0
        } else {
            error_count as f64 / count as f64
        };

        let window_count = count.min(WINDOW_SIZE as u64) as usize;
        let (p50_ns, p95_ns, p99_ns, p999_ns) = if window_count == 0 {
            (0, 0, 0, 0)
        } else {
            let mut samples: Vec<u64> = (0..window_count)
                .map(|i| self.window[i].load(Ordering::Relaxed))
                .collect();
            samples.sort_unstable();
            (
                percentile(&samples, 50.0),
                percentile(&samples, 95.0),
                percentile(&samples, 99.0),
                percentile(&samples, 99.9),
            )
        };

        StageSnapshot {
            label: self.label.clone(),
            count,
            error_count,
            error_rate,
            last_ns: self.last_ns.load(Ordering::Relaxed),
            min_ns,
            max_ns,
            mean_ns,
            p50_ns,
            p95_ns,
            p99_ns,
            p999_ns,
        }
    }
}

/// Wraps a stage with nanosecond timing, recording to a [`StageMetrics`] instance.
///
/// The metrics label is derived automatically from [`Stage::name`]. Construction
/// returns the wrapper and its [`StageMetrics`] together; register the metrics
/// with [`PipelineMetrics`] if needed.
///
/// # Example
/// ```
/// use pipexec::metrics::Timed;
/// use pipexec::stage::Stage;
/// use pipexec::scratchpad::Scratchpad;
/// use pipexec::error::PipelineError;
/// use pipexec::dynamic_pipeline::Pipeline;
///
/// struct MyScratchpad;
///
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) {}
/// }
///
/// struct MyStage;
///
/// impl Stage<MyScratchpad> for MyStage {
///     fn run(&mut self, _ctx: &mut MyScratchpad) -> Result<(), PipelineError> {
///         Ok(())
///     }
/// }
///
/// let (my_stage, my_stage_metrics) = Timed::new(MyStage);
/// let mut pipeline = Pipeline::new().stage(my_stage);
/// let mut ctx = MyScratchpad;
/// pipeline.run(&mut ctx).unwrap();
/// assert_eq!(my_stage_metrics.snapshot().count, 1);
/// ```
#[derive(Debug)]
pub struct Timed<S: Scratchpad, T: Stage<S>> {
    stage: T,
    metrics: Arc<StageMetrics>,
    _marker: std::marker::PhantomData<fn(S) -> S>,
}

impl<S: Scratchpad, T: Stage<S>> Timed<S, T> {
    /// Wraps `stage` with timing, returning the wrapper and its metrics collector.
    ///
    /// The metrics label is derived from [`Stage::name`], which defaults to the
    /// fully qualified type name. Override [`Stage::name`] on your stage type to
    /// use a shorter label.
    ///
    /// ```
    /// # use pipexec::metrics::Timed;
    /// # use pipexec::stage::Stage;
    /// # use pipexec::scratchpad::Scratchpad;
    /// # use pipexec::error::PipelineError;
    /// # struct S; impl Scratchpad for S { fn reset(&mut self) {} }
    /// # struct MyStage; impl Stage<S> for MyStage { fn run(&mut self, _: &mut S) -> Result<(), PipelineError> { Ok(()) } }
    /// let (my_stage, my_stage_metrics) = Timed::new(MyStage);
    /// ```
    #[must_use]
    pub fn new(stage: T) -> (Self, Arc<StageMetrics>) {
        let name = stage.name();
        let metrics = StageMetrics::new(name);
        let wrapper = Self {
            stage,
            metrics: Arc::clone(&metrics),
            _marker: std::marker::PhantomData,
        };
        (wrapper, metrics)
    }
}

impl<S: Scratchpad + Send, T: Stage<S>> Stage<S> for Timed<S, T> {
    fn name(&self) -> &'static str {
        self.stage.name()
    }

    #[inline]
    fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        let start = Instant::now();
        let result = self.stage.run(ctx);
        let failed = result.is_err();
        self.metrics
            .record(start.elapsed().as_nanos() as u64, failed);
        result
    }
}

fn fetch_min(atomic: &AtomicU64, value: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    while value < current {
        match atomic.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn fetch_max(atomic: &AtomicU64, value: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    while value > current {
        match atomic.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

/// A point-in-time snapshot of metrics for all tracked stages in a pipeline.
#[derive(Debug, Clone)]
pub struct PipelineSnapshot {
    /// Snapshots for each tracked stage, in registration order.
    pub stages: Vec<StageSnapshot>,
}

impl PipelineSnapshot {
    /// Returns the stage with the highest p99 latency, or `None` if empty.
    #[must_use]
    pub fn slowest_stage(&self) -> Option<&StageSnapshot> {
        self.stages.iter().max_by_key(|s| s.p99_ns)
    }

    /// Returns all stages that have recorded at least one error.
    pub fn error_stages(&self) -> impl Iterator<Item = &StageSnapshot> {
        self.stages.iter().filter(|s| s.error_count > 0)
    }

    /// Returns the total execution count across all stages.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.stages.iter().map(|s| s.count).sum()
    }
}

impl std::fmt::Display for PipelineSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for stage in &self.stages {
            writeln!(f, "{}", stage.summary())?;
        }
        Ok(())
    }
}

/// Collects and aggregates metrics for all stages in a pipeline.
///
/// Create one `PipelineMetrics` per pipeline, register each stage via
/// [`register`][PipelineMetrics::register], and call [`snapshot`][PipelineMetrics::snapshot]
/// to read all stage metrics in a single call.
///
/// # Example
/// ```
/// use pipexec::metrics::{PipelineMetrics, Timed};
/// use pipexec::dynamic_pipeline::Pipeline;
/// use pipexec::scratchpad::Scratchpad;
/// use pipexec::stage::Stage;
/// use pipexec::error::PipelineError;
///
/// struct MyScratchpad;
/// impl Scratchpad for MyScratchpad {
///     fn reset(&mut self) {}
/// }
///
/// struct NoopStage;
/// impl Stage<MyScratchpad> for NoopStage {
///     fn run(&mut self, _ctx: &mut MyScratchpad) -> Result<(), PipelineError> { Ok(()) }
/// }
///
/// let mut pm = PipelineMetrics::new();
/// let (noop, noop_metrics) = Timed::new(NoopStage);
/// pm.register(noop_metrics);
///
/// let mut pipeline = Pipeline::new().stage(noop);
/// let mut ctx = MyScratchpad;
/// pipeline.run(&mut ctx).unwrap();
///
/// let snapshot = pm.snapshot();
/// assert_eq!(snapshot.stages.len(), 1);
/// assert_eq!(snapshot.total_count(), 1);
/// ```
#[derive(Debug, Default)]
pub struct PipelineMetrics {
    metrics: Vec<Arc<StageMetrics>>,
}

impl PipelineMetrics {
    /// Creates a new empty `PipelineMetrics` collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a pre-existing [`StageMetrics`] for tracking.
    ///
    /// Use this with [`Timed::new`] to derive the label automatically and
    /// still include the stage in a [`PipelineSnapshot`]:
    ///
    /// ```
    /// use pipexec::metrics::{PipelineMetrics, Timed};
    /// use pipexec::stage::Stage;
    /// use pipexec::scratchpad::Scratchpad;
    /// use pipexec::error::PipelineError;
    /// use pipexec::dynamic_pipeline::Pipeline;
    ///
    /// struct MyScratchpad;
    /// impl Scratchpad for MyScratchpad {
    ///     fn reset(&mut self) {}
    /// }
    ///
    /// struct MyStage;
    /// impl Stage<MyScratchpad> for MyStage {
    ///     fn run(&mut self, _ctx: &mut MyScratchpad) -> Result<(), PipelineError> { Ok(()) }
    /// }
    ///
    /// let mut pm = PipelineMetrics::new();
    /// let (my_stage, my_stage_metrics) = Timed::new(MyStage);
    /// pm.register(my_stage_metrics);
    /// let mut pipeline = Pipeline::new().stage(my_stage);
    /// let mut ctx = MyScratchpad;
    /// pipeline.run(&mut ctx).unwrap();
    /// assert_eq!(pm.snapshot().total_count(), 1);
    /// ```
    pub fn register(&mut self, metrics: Arc<StageMetrics>) {
        self.metrics.push(metrics);
    }

    /// Returns a point-in-time snapshot of all tracked stages.
    #[must_use]
    pub fn snapshot(&self) -> PipelineSnapshot {
        PipelineSnapshot {
            stages: self.metrics.iter().map(|m| m.snapshot()).collect(),
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0 * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestScratchpad;

    impl Scratchpad for TestScratchpad {
        fn reset(&mut self) {}
    }

    struct NoopStage;

    impl Stage<TestScratchpad> for NoopStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            Ok(())
        }
    }

    struct FailStage;

    impl Stage<TestScratchpad> for FailStage {
        fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
            Err(PipelineError::StageFailed {
                stage: "FailStage",
                source: "fail".into(),
            })
        }
    }

    #[test]
    fn records_execution_count() {
        let (mut stage, noop_metrics) = Timed::new(NoopStage);
        let mut ctx = TestScratchpad;

        for _ in 0..10 {
            stage.run(&mut ctx).unwrap();
        }

        assert_eq!(noop_metrics.snapshot().count, 10);
    }

    #[test]
    fn records_error_count() {
        let (mut stage, fail_metrics) = Timed::new(FailStage);
        let mut ctx = TestScratchpad;

        for _ in 0..5 {
            stage.run(&mut ctx).ok();
        }

        let snapshot = fail_metrics.snapshot();
        assert_eq!(snapshot.error_count, 5);
        assert!((snapshot.error_rate - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn error_propagates_through_timed_wrapper() {
        let (mut stage, _) = Timed::new(FailStage);
        let mut ctx = TestScratchpad;

        assert!(matches!(
            stage.run(&mut ctx),
            Err(PipelineError::StageFailed { .. })
        ));
    }

    #[test]
    fn min_is_less_than_or_equal_to_max() {
        let (mut stage, noop_metrics) = Timed::new(NoopStage);
        let mut ctx = TestScratchpad;

        for _ in 0..100 {
            stage.run(&mut ctx).unwrap();
        }

        let snapshot = noop_metrics.snapshot();
        assert!(snapshot.min_ns <= snapshot.max_ns);
    }

    #[test]
    fn percentiles_are_ordered() {
        let (mut stage, noop_metrics) = Timed::new(NoopStage);
        let mut ctx = TestScratchpad;

        for _ in 0..200 {
            stage.run(&mut ctx).unwrap();
        }

        let s = noop_metrics.snapshot();
        assert!(s.p50_ns <= s.p95_ns);
        assert!(s.p95_ns <= s.p99_ns);
        assert!(s.p99_ns <= s.p999_ns);
    }

    #[test]
    fn mixed_success_and_failure_error_rate() {
        struct SometimesFailStage(u32);
        impl Stage<TestScratchpad> for SometimesFailStage {
            fn run(&mut self, _ctx: &mut TestScratchpad) -> Result<(), PipelineError> {
                self.0 += 1;
                if self.0 % 4 == 0 {
                    Err(PipelineError::StageFailed {
                        stage: "SometimesFailStage",
                        source: "scheduled".into(),
                    })
                } else {
                    Ok(())
                }
            }
        }

        let (mut stage, sometimes_metrics) = Timed::new(SometimesFailStage(0));
        let mut ctx = TestScratchpad;

        for _ in 0..4 {
            stage.run(&mut ctx).ok();
        }

        let snapshot = sometimes_metrics.snapshot();
        assert_eq!(snapshot.count, 4);
        assert_eq!(snapshot.error_count, 1);
        assert!((snapshot.error_rate - 0.25).abs() < 1e-9);
    }
}
