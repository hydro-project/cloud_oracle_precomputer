use hydroflow::util::cli::{ConnectedDirect, ConnectedSource};
use hydroflow::util::deserialize_from_bytes;
use hydroflow::hydroflow_syntax;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;

    let input_recv = ports
        .port("input")
        // connect to the port with a single recipient
        .connect::<ConnectedDirect>() 
        .await
        .into_source();

    let mut output_monitor = MonitorMovingAverage::new(1000);

    type Input = skypie_lib::skypie_lib::candidate_policies_hydroflow::OutputType;

    let flow = hydroflow_syntax! {

        input = source_stream(input_recv) -> map(|x| -> Input {deserialize_from_bytes(x.unwrap()).unwrap()});
        /* input -> fold::<'static>(0, |mut acc, _x| {
            acc += 1;
            acc
        }) */
        input  -> for_each(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Decisions:", Some(1000));
            //println!("Decisions: {}", output_monitor);
            /* if output_monitor.get_count() % 1 == 0 {
                println!("{:?} outputs, at rate {:?}", output_monitor.get_count(), output_monitor.get_arrival_time_average().unwrap());
            } */
        });
    };

    println!("Launching");
    hydroflow::util::cli::launch_flow(flow).await;
    println!("Stopping");
}