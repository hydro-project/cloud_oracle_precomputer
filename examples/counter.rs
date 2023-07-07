use hydroflow::util::cli::{ConnectedDirect, ConnectedSource};
use hydroflow::hydroflow_syntax;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;

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

    let mut output_monitor = MonitorMovingAverage::new(1000);

    let flow = hydroflow_syntax! {
        
        input = source_stream(input_recv);
        input  -> for_each(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Decisions:", Some(1000));
        });
    };

    println!("Launching");
    hydroflow::util::cli::launch_flow(flow).await;
    println!("Stopping");
}