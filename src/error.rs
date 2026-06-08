//! Error type for pipeline execution failures.
//!
//! [`PipelineError`] is `#[non_exhaustive]`: always include a wildcard arm
//! when matching to remain compatible with future variants. For chained errors
//! such as [`PipelineError::RetryExhausted`], the original error is accessible
//! via [`std::error::Error::source`].

/// Represents errors that can occur during pipeline execution.
#[derive(Debug)]
#[non_exhaustive]
pub enum PipelineError {
    /// A stage failed during execution.
    ///
    /// `stage` is the name of the stage type or function that failed.
    /// `message` describes what went wrong.
    StageFailed {
        /// Name of the stage type or function that failed.
        stage: &'static str,
        /// Human-readable description of what went wrong.
        message: String,
    },

    /// The pipeline has no stages to execute.
    EmptyPipeline,

    /// The pipeline has no more room to grow.
    FullPipeline,

    /// The scratchpad or pipeline configuration was in an unexpected state.
    ///
    /// `context` identifies where the unexpected state was detected.
    /// `message` describes what was wrong.
    InvalidState {
        /// Identifies where the unexpected state was detected.
        context: &'static str,
        /// Human-readable description of what was wrong.
        message: String,
    },

    /// A stage failed after exhausting all retry attempts.
    ///
    /// Carries the number of attempts made and the last error returned by the stage.
    /// The original error is accessible via [`std::error::Error::source`].
    RetryExhausted {
        /// Total number of attempts made before giving up.
        attempts: u32,
        /// The last error returned by the stage.
        source: Box<PipelineError>,
    },

    /// A stage completed successfully but exceeded its time budget.
    ///
    /// Carries the budget and actual elapsed time, both in nanoseconds.
    DeadlineExceeded {
        /// Time budget in nanoseconds.
        budget_ns: u64,
        /// Actual elapsed time in nanoseconds.
        elapsed_ns: u64,
    },
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::StageFailed { stage, message } => {
                write!(f, "stage '{stage}' failed: {message}")
            }
            PipelineError::EmptyPipeline => write!(f, "pipeline has no stages"),
            PipelineError::FullPipeline => write!(f, "pipeline has no more room to grow"),
            PipelineError::InvalidState { context, message } => {
                write!(f, "invalid state in {context}: {message}")
            }
            PipelineError::RetryExhausted { attempts, source } => {
                write!(f, "stage failed after {attempts} attempts: {source}")
            }
            PipelineError::DeadlineExceeded {
                budget_ns,
                elapsed_ns,
            } => {
                write!(
                    f,
                    "stage exceeded deadline: budget {budget_ns}ns, elapsed {elapsed_ns}ns"
                )
            }
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipelineError::RetryExhausted { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_failed_contains_message() {
        let error = PipelineError::StageFailed {
            stage: "test",
            message: String::from("something went wrong"),
        };
        assert!(format!("{error:?}").contains("something went wrong"));
    }

    #[test]
    fn empty_pipeline_is_debug_printable() {
        let error = PipelineError::EmptyPipeline;
        assert!(format!("{error:?}").contains("EmptyPipeline"));
    }

    #[test]
    fn full_pipeline_is_debug_printable() {
        let error = PipelineError::FullPipeline;
        assert!(format!("{error:?}").contains("FullPipeline"));
    }

    #[test]
    fn full_pipeline_display_message() {
        let error = PipelineError::FullPipeline;
        assert_eq!(format!("{error}"), "pipeline has no more room to grow");
    }

    #[test]
    fn retry_exhausted_carries_source_error() {
        let error = PipelineError::RetryExhausted {
            attempts: 3,
            source: Box::new(PipelineError::StageFailed {
                stage: "test",
                message: String::from("timed out"),
            }),
        };
        assert!(format!("{error:?}").contains('3'));
        assert!(format!("{error}").contains("timed out"));
        assert!(std::error::Error::source(&error).is_some());
    }
}
