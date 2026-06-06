//! Tuple-based stage composition for inline zero-allocation pipelines.
//!
//! Implements [`Stage<S>`][crate::stage::Stage] for tuples of up to 16 stages,
//! allowing a tuple to act as a self-contained pipeline. All stage state is
//! stored inline in the tuple — no heap allocation, no dynamic dispatch, no
//! capacity constant. Wrappers (`Timed`, `Retry`, `Deadline`, `Instrumented`)
//! are just tuple elements.
//!
//! # Example
//!
//! ```rust
//! use pipex::stage::Stage;
//! use pipex::scratchpad::Scratchpad;
//! use pipex::error::PipelineError;
//! use pipex::metrics::Timed;
//!
//! struct Buf {
//!     value: f32,
//! }
//!
//! impl Scratchpad for Buf {
//!     fn reset(&mut self) {
//!         self.value = 0.0;
//!     }
//! }
//!
//! struct Double;
//! struct Clamp;
//!
//! impl Stage<Buf> for Double {
//!     fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
//!         ctx.value *= 2.0;
//!         Ok(())
//!     }
//! }
//!
//! impl Stage<Buf> for Clamp {
//!     fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
//!         ctx.value = ctx.value.clamp(0.0, 10.0);
//!         Ok(())
//!     }
//! }
//!
//! let (clamp, _clamp_metrics) = Timed::new(Clamp);
//! let mut pipeline = (Double, clamp);
//! let mut ctx = Buf { value: 3.0 };
//! pipeline.run(&mut ctx).unwrap();
//! ```

use crate::error::PipelineError;
use crate::scratchpad::Scratchpad;
use crate::stage::Stage;

macro_rules! impl_stage_for_tuple {
    ($($idx:tt: $T:ident),+) => {
        impl<S: Scratchpad, $($T: Stage<S>),+> Stage<S> for ($($T,)+) {
            fn run(&mut self, ctx: &mut S) -> Result<(), PipelineError> {
                $(self.$idx.run(ctx)?;)+
                Ok(())
            }
        }
    };
}

impl_stage_for_tuple!(0: T0);
impl_stage_for_tuple!(0: T0, 1: T1);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10, 11: T11);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10, 11: T11, 12: T12);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10, 11: T11, 12: T12, 13: T13);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10, 11: T11, 12: T12, 13: T13, 14: T14);
impl_stage_for_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7, 8: T8, 9: T9, 10: T10, 11: T11, 12: T12, 13: T13, 14: T14, 15: T15);

#[cfg(test)]
mod tests {
    use super::*;

    struct Buf {
        value: f32,
    }

    impl Scratchpad for Buf {
        fn reset(&mut self) {
            self.value = 0.0;
        }
    }

    struct Double;
    struct AddOne;
    struct Clamp;
    struct Fail;

    impl Stage<Buf> for Double {
        fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
            ctx.value *= 2.0;
            Ok(())
        }
    }

    impl Stage<Buf> for AddOne {
        fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
            ctx.value += 1.0;
            Ok(())
        }
    }

    impl Stage<Buf> for Clamp {
        fn run(&mut self, ctx: &mut Buf) -> Result<(), PipelineError> {
            ctx.value = ctx.value.clamp(0.0, 5.0);
            Ok(())
        }
    }

    impl Stage<Buf> for Fail {
        fn run(&mut self, _ctx: &mut Buf) -> Result<(), PipelineError> {
            Err(PipelineError::StageFailed {
                stage: "Fail",
                message: String::from("intentional"),
            })
        }
    }

    #[test]
    fn single_stage_tuple_runs() {
        let mut pipeline = (Double,);
        let mut ctx = Buf { value: 3.0 };
        pipeline.run(&mut ctx).unwrap();
        assert_eq!(ctx.value, 6.0);
    }

    #[test]
    fn two_stage_tuple_runs_in_order() {
        let mut pipeline = (Double, AddOne);
        let mut ctx = Buf { value: 3.0 };
        pipeline.run(&mut ctx).unwrap();
        assert_eq!(ctx.value, 7.0); // (3 * 2) + 1
    }

    #[test]
    fn three_stage_tuple_runs_in_order() {
        let mut pipeline = (Double, AddOne, Clamp);
        let mut ctx = Buf { value: 3.0 };
        pipeline.run(&mut ctx).unwrap();
        assert_eq!(ctx.value, 5.0); // clamp(7.0, 0.0, 5.0)
    }

    #[test]
    fn error_stops_execution() {
        let mut pipeline = (Double, Fail, AddOne);
        let mut ctx = Buf { value: 1.0 };
        assert!(pipeline.run(&mut ctx).is_err());
        assert_eq!(ctx.value, 2.0); // Double ran, Fail stopped it, AddOne did not run
    }

    #[test]
    fn tuple_usable_as_stage_in_dynamic_pipeline() {
        use crate::dynamic_pipeline::Pipeline;
        let inner = (Double, AddOne);
        let mut pipeline = Pipeline::new().stage(inner);
        let mut ctx = Buf { value: 2.0 };
        pipeline.run(&mut ctx).unwrap();
        assert_eq!(ctx.value, 5.0); // (2 * 2) + 1
    }
}
