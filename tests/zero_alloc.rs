use pipexec::dynamic_pipeline::Pipeline as DynamicPipeline;
use pipexec::error::PipelineError;
use pipexec::scratchpad::Scratchpad;
use pipexec::stage::Stage;
use pipexec::static_pipeline::Pipeline as StaticPipeline;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<usize> = const { Cell::new(0) };
}

struct TrackingAllocator;

impl TrackingAllocator {
    fn start(&self) {
        TRACKING.with(|t| t.set(true));
        COUNT.with(|c| c.set(0));
    }

    fn count(&self) -> usize {
        COUNT.with(|c| c.get())
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACKING.try_with(|t| t.get()).unwrap_or(false) {
            let _ = COUNT.try_with(|c| c.set(c.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

struct ZeroAllocScratchpad {
    values: Vec<f32>,
}

impl Scratchpad for ZeroAllocScratchpad {
    fn reset(&mut self) {
        self.values.iter_mut().for_each(|x| *x = 0.0);
    }
}

struct ScaleStage;

impl Stage<ZeroAllocScratchpad> for ScaleStage {
    fn run(&mut self, ctx: &mut ZeroAllocScratchpad) -> Result<(), PipelineError> {
        ctx.values.iter_mut().for_each(|x| *x *= 2.0);
        Ok(())
    }
}

fn scale(ctx: &mut ZeroAllocScratchpad) -> Result<(), PipelineError> {
    ctx.values.iter_mut().for_each(|x| *x *= 2.0);
    Ok(())
}

// Retry is intentionally absent from these tests. It clones the scratchpad
// before each attempt (for state restoration on failure), which allocates for
// heap-bearing scratchpads. See the Retry doc comment for details.
mod zero_alloc_tests {
    use super::*;

    #[test]
    fn dynamic_pipeline_does_not_allocate_during_run() {
        let mut pipeline = DynamicPipeline::new().stage(ScaleStage);
        let mut ctx = ZeroAllocScratchpad {
            values: vec![1.0, 2.0, 3.0],
        };

        ALLOCATOR.start();
        pipeline.run(&mut ctx).unwrap();

        assert_eq!(
            ALLOCATOR.count(),
            0,
            "dynamic pipeline allocated during run"
        );
    }

    #[test]
    fn static_pipeline_does_not_allocate_during_run() {
        let mut pipeline = StaticPipeline::<ZeroAllocScratchpad, 1>::new();
        pipeline.add_stage(scale).unwrap();
        let mut ctx = ZeroAllocScratchpad {
            values: vec![1.0, 2.0, 3.0],
        };

        ALLOCATOR.start();
        pipeline.run(&mut ctx).unwrap();

        assert_eq!(ALLOCATOR.count(), 0, "static pipeline allocated during run");
    }
}
