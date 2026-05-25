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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_failed_contains_message() {
        let error = PipelineError::StageFailed(String::from("something went wrong"));
        assert!(format!("{:?}", error).contains("something went wrong"));
    }

    #[test]
    fn validation_failed_contains_message() {
        let error = PipelineError::ValidationFailed(String::from("invalid state"));
        assert!(format!("{:?}", error).contains("invalid state"));
    }

    #[test]
    fn empty_pipeline_is_debug_printable() {
        let error = PipelineError::EmptyPipeline;
        assert!(format!("{:?}", error).contains("EmptyPipeline"));
    }

    #[test]
    fn retry_exhausted_contains_attempts_and_reason() {
        let error = PipelineError::RetryExhausted {
            attempts: 3,
            reason: String::from("timed out"),
        };
        let debug = format!("{:?}", error);
        assert!(debug.contains("3"));
        assert!(debug.contains("timed out"));
    }
}