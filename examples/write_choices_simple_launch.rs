use itertools::Itertools;
use clap::Parser;

use hydroflow::util::cli::{ConnectedDirect, ConnectedSink};
use hydroflow::util::{serialize_to_bytes};
use hydroflow::hydroflow_syntax;
use skypie_lib::Loader;
use skypie_lib::skypie_lib::args::Args;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;

    // Load the input
    let args = Args::parse();

    let loader = Loader::new(&args.network_file, &args.object_store_file, &args.region_selector);
    
    // Get ports
    let output_send = ports
        .port("output")
        .connect::<ConnectedDirect>() 
        .await
        .into_sink();

    //let regions = loader.regions;
    let object_stores = loader.object_stores;
    let replication_factor = args.replication_factor;
    //let batch_size = args.batch_size;

    let mut output_monitor = MonitorMovingAverage::new(1000);

    let flow = hydroflow_syntax!{
        write_choices = source_iter(object_stores.into_iter().combinations(replication_factor));
        // Distribute the write choices among instances of next stage
        write_choices -> map(|x| skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType{object_stores: x})
        -> map(|x:skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType| serialize_to_bytes(x))
        -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Write choices:", Some(1000));
        })
        -> dest_sink(output_send);
    };

    hydroflow::util::cli::launch_flow(flow).await;
    println!("DONE: write choices sent");
}