#![feature(test)]
extern crate test;

mod skypie_lib;

mod args;

use clap::Parser;
use skypie_lib::skypie_precomputation::skypie_precomputation;
use crate::{skypie_lib::loader::Loader, args::Args};


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