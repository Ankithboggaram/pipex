#![no_main]

use libfuzzer_sys::fuzz_target;
use pipex::deadline::Deadline;
use pipex::dynamic_pipeline::Pipeline;
use pipex::error::PipelineError;
use pipex::retry::Retry;
use pipex::scratchpad::Scratchpad;
use pipex::stage::Stage;
use std::time::Duration;

#[derive(Clone)]
struct FuzzScratchpad {
    value: i32,
}

impl Scratchpad for FuzzScratchpad {
    fn reset(&mut self) {
        self.value = 0;
    }
}

struct Increment;

impl Stage<FuzzScratchpad> for Increment {
    fn run(&mut self, ctx: &mut FuzzScratchpad) -> Result<(), PipelineError> {
        ctx.value = ctx.value.wrapping_add(1);
        Ok(())
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 3 {
        return;
    }

    // Byte 0: selects wrapper composition.
    // Byte 1: retry count (1-5).
    // Byte 2: deadline budget in 100us units (100us-25.6ms).
    let composition = data[0] % 4;
    let retry_count = (data[1] % 5) as u32 + 1;
    let deadline_us = (data[2] as u64 + 1) * 100;

    let mut pipeline = match composition {
        0 => Pipeline::new().stage(Increment),
        1 => Pipeline::new().stage(Retry::new(Increment, retry_count)),
        2 => Pipeline::new().stage(Deadline::new(
            Increment,
            Duration::from_micros(deadline_us),
        )),
        _ => Pipeline::new().stage(Retry::new(
            Deadline::new(Increment, Duration::from_micros(deadline_us)),
            retry_count,
        )),
    };

    let mut ctx = FuzzScratchpad { value: 0 };
    let _ = pipeline.run(&mut ctx);
});
