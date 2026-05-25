//! Error types for pipeline execution.

/// Represents errors that can occur during pipeline execution.
#[derive(Debug)]
pub enum PipelineError {
    /// A stage failed during execution, with a descriptive message.
    StageFailed(String),

    /// The scratchpad failed validation before execution began.
    ValidationFailed(String),

    /// The pipeline has no stages to execute.
    EmptyPipeline,

    /// The scratchpad was in an unexpected state during execution.
    InvalidState(String),

    /// A stage failed after exhausting all retry attempts.
    ///
    /// Carries the number of attempts made and the final error message.
    RetryExhausted { attempts: u32, reason: String },
}
