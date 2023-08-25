use std::path::PathBuf;

use clap::Parser;
use hydroflow::util::cli::{ConnectedDirect, ConnectedSource, ConnectedSink};

use skypie_lib::{candidate_policies_reduce_hydroflow, Args, Loader, ApplicationRegion};

#[hydroflow::main]
async fn main() {
    // XXX: This must be called as the very first thing in the program!!!!!!!
    let mut ports = hydroflow::util::cli::init().await;

    let input_recv = ports
        .port("input")
        // connect to the port with a single recipient
        .connect::<ConnectedDirect>() 
        .await
        .into_source();

    let time_sink = ports
        .port("time_output")
        .connect::<ConnectedDirect>() 
        .await
        .into_sink();

    // Load the input
    let args = Args::parse();

    let loader = Loader::new(&args.network_file, &args.object_store_file, &args.region_selector);

    // Static life time hack for hydroflow lifetime mess
    let data = Box::new(loader.app_regions);
    let regions: &'static Vec<ApplicationRegion> = Box::leak(data);

    let output_candidates_file_name: String = args.output_candidates_file_name.unwrap_or(PathBuf::from("/dev/null")).to_str().unwrap().into();
    let output_file_name = args.output_file_name.unwrap_or(PathBuf::from("/dev/null")).to_str().unwrap().into();

    let object_store_id_map = loader.object_stores.iter().map(|x| (x.id.clone(), x.clone())).collect::<std::collections::HashMap<_,_>>();

    let flow = candidate_policies_reduce_hydroflow(regions, input_recv, args.batch_size, args.experiment_name, output_candidates_file_name, output_file_name, object_store_id_map, time_sink);

    println!("Launching candidate and reduce");
    hydroflow::util::cli::launch_flow(flow).await;
}