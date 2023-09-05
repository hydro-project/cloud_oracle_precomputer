#![feature(exclusive_range_pattern)]
#![feature(test)]
extern crate test;

#[macro_use]
extern crate lazy_static;

pub mod region;
pub mod object_store;
pub mod application_region;

mod identifier;

pub mod merge_policies;
pub mod opt_assignments;
//pub mod candidate_policies;
pub mod reduce_oracle;
pub mod network_record;
//pub mod combinations;
pub mod read_choice;
pub mod range;

pub mod decision;
pub mod write_choice;
pub mod args;
pub mod loader;
//pub mod skypie_precomputation;
pub mod candidate_policies_hydroflow;
pub mod candidate_policies_and_reduce_hydroflow;
pub mod monitor;
pub mod reduce_oracle_hydroflow;
//pub mod combinations_wrapper;
pub mod influx_logger;
pub mod noop_logger;
pub mod log_entry;
pub mod output;
pub mod iter_stream_batches;
pub mod optimizer_stats;
pub mod tombstone;

pub use loader::Loader;
pub use candidate_policies_hydroflow::candidate_policies_hydroflow;
pub use candidate_policies_and_reduce_hydroflow::candidate_policies_reduce_hydroflow;
pub use args::Args;
pub use region::Region;
pub use write_choice::WriteChoice;
pub use decision::Decision;
pub use application_region::ApplicationRegion;
//pub use influx_logger;
pub use log_entry::SkyPieLogEntry;
pub use tombstone::Tombstone;