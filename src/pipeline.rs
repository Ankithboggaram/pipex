//! Pipeline executor that runs stages against a scratchpad.

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

/// A pipeline that runs a sequence of stages against a scratchpad.
///
/// # Example
/// ```
/// use pipex::pipeline::Pipeline;
///
/// let pipeline = Pipeline::new()
///     .with_retries(3);
/// ```
pub struct Pipeline<S: Scratchpad> {
    /// The sequence of stages to execute.
    stages: Vec<Box<dyn Stage<S>>>,
    /// The number of times to retry a failed stage before giving up.
    retries: u32,
}

impl<S: Scratchpad> Pipeline<S> {
    /// Creates a new empty pipeline with no retries.
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            retries: 0,
        }
    }

    /// Sets the number of retries for failed stages.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Adds a stage to the pipeline.
    pub fn add_stage<T: Stage<S> + 'static>(&mut self, stage: T) {
        self.stages.push(Box::new(stage));
    }

    /// Runs all stages in order against the provided scratchpad.
    ///
    /// Validates the scratchpad before execution, and resets it
    /// between pipeline runs.
    pub fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
        if self.stages.is_empty() {
            return Err(PipelineError::EmptyPipeline);
        }

        if !ctx.validate() {
            return Err(PipelineError::ValidationFailed(
                String::from("Scratchpad failed validation before pipeline execution"),
            ));
        }

        for stage in self.stages.iter_mut() {
            let mut last_error = None;

            for attempt in 0..=self.retries {
                match stage.run(ctx) {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e);
                        if attempt < self.retries {
                            ctx.reset();
                        }
                    }
                }
            }

            if let Some(e) = last_error {
                return Err(PipelineError::RetryExhausted {
                    attempts: self.retries + 1,
                    reason: format!("{:?}", e),
                });
            }
        }

        Ok(())
    }
}