#![feature(exclusive_range_pattern)]
#![feature(test)]
extern crate test;

#[macro_use]
extern crate lazy_static;

pub mod skypie_lib;

//pub use skypie_lib::skypie_precomputation::skypie_precomputation;
pub use skypie_lib::loader::Loader;
pub use skypie_lib::candidate_policies_hydroflow::candidate_policies_hydroflow;
pub use skypie_lib::candidate_policies_and_reduce_hydroflow::candidate_policies_reduce_hydroflow;
pub use skypie_lib::args::Args;
pub use skypie_lib::region::Region;
pub use skypie_lib::write_choice::WriteChoice;
pub use skypie_lib::decision::Decision;
pub use skypie_lib::application_region::ApplicationRegion;
pub use skypie_lib::influx_logger;
pub use skypie_lib::log_entry::SkyPieLogEntry;