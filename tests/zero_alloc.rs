use pipex::dynamic_pipeline::Pipeline as DynamicPipeline;
use pipex::error::PipelineError;
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use pipex::static_pipeline::Pipeline as StaticPipeline;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct TrackingAllocator {
    allocations: AtomicUsize,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            allocations: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.allocations.store(0, Ordering::SeqCst);
    }

    fn count(&self) -> usize {
        self.allocations.load(Ordering::SeqCst)
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocations.fetch_add(1, Ordering::SeqCst);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

struct ZeroAllocScratchpad {
    values: Vec<f32>,
}

impl Scratchpad for ZeroAllocScratchpad {
    fn reset(&mut self) {
        self.values.iter_mut().for_each(|x| *x = 0.0);
    }

    fn validate(&self) -> bool {
        !self.values.is_empty()
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

mod zero_alloc_tests {
    use super::*;

    #[test]
    fn dynamic_pipeline_does_not_allocate_during_run() {
        let mut pipeline = DynamicPipeline::new();
        pipeline.add_stage(ScaleStage);
        let mut ctx = ZeroAllocScratchpad {
            values: vec![1.0, 2.0, 3.0],
        };

        // warm up — first run triggers validation flag write
        pipeline.run(&mut ctx).unwrap();

        // reset counter and measure only subsequent runs
        ALLOCATOR.reset();
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

        pipeline.run(&mut ctx).unwrap();

        ALLOCATOR.reset();
        pipeline.run(&mut ctx).unwrap();

        assert_eq!(ALLOCATOR.count(), 0, "static pipeline allocated during run");
    }
}
