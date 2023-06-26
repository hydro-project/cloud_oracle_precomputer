#![feature(test)]
extern crate test;

use clap::Parser;
use crate::region::Region;

mod region;
mod object_store;
use crate::object_store::ObjectStore;

mod merge_policies;
mod opt_assignments;
mod candidate_policies;
mod candidate_policies_hydroflow;
mod reduce_oracle;
mod loader;
use crate::loader::Loader;
mod network_record;
mod args;
use crate::args::Args;
mod combinations;
mod skypie_precomputation;
use crate::skypie_precomputation::skypie_precomputation;

mod decision;
use crate::decision::Decision;
mod read_choice;
use crate::read_choice::ReadChoice;
mod write_choice;
use crate::write_choice::WriteChoice;
mod range;


pub fn main() {

    let args = Args::parse();

    let loader = Loader::new(&args.network_file, &args.object_store_file, &args.region_selector);
    
    let regions = loader.regions;
    let object_stores = loader.object_stores;
    let replication_factor = args.replication_factor;
    let batch_size = args.batch_size;

    let decisions = skypie_precomputation(regions, object_stores, replication_factor, batch_size);

    println!("Decisions {:?}", decisions.len());
}