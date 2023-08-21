use clap::Parser;
use hydroflow::{util::cli::{ConnectedDirect, ConnectedSink, ConnectedSource}};

use skypie_lib::{candidate_policies_hydroflow, Args, Loader, ApplicationRegion};

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;
    
    eprintln!("Starting candidate_policies_launch.rs");

    // Load the input
    let args = Args::parse();

    let loader = Loader::new(&args.network_file, &args.object_store_file, &args.region_selector);


    let input_recv = ports
        .port("input")
        // connect to the port with a single recipient
        .connect::<ConnectedDirect>() 
        .await
        .into_source();

    let output_send = ports
        .port("output")
        .connect::<ConnectedDirect>() 
        .await
        .into_sink();

    // Static life time hack for hydroflow lifetime mess
    let data = Box::new(loader.app_regions);
    let regions: &'static Vec<ApplicationRegion> = Box::leak(data);

    let flow = candidate_policies_hydroflow(regions, input_recv, output_send);

    eprintln!("Launching candidate_policies_launch.rs");
    hydroflow::util::cli::launch_flow(flow).await;
}