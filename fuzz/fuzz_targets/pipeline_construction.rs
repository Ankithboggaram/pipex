#![no_main]

use libfuzzer_sys::fuzz_target;
use pipex::error::PipelineError;
use pipex::scratchpad::Scratchpad;
use pipex::static_pipeline::Pipeline;

struct FuzzScratchpad {
    value: u64,
}

impl Scratchpad for FuzzScratchpad {
    fn reset(&mut self) {
        self.value = 0;
    }
}

fn increment(ctx: &mut FuzzScratchpad) -> Result<(), PipelineError> {
    ctx.value = ctx.value.wrapping_add(1);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // First byte drives stage count; remaining bytes set the initial scratchpad value.
    let stage_count = data[0] as usize;
    let initial = if data.len() >= 9 {
        u64::from_le_bytes(data[1..9].try_into().unwrap())
    } else {
        0
    };

    let mut pipeline = Pipeline::<FuzzScratchpad, 16>::new();
    for _ in 0..stage_count {
        // Excess stages beyond capacity must return an error, never panic.
        let _ = pipeline.add_stage(increment);
    }

    let mut ctx = FuzzScratchpad { value: initial };
    let _ = pipeline.run(&mut ctx);
});
