use hydroflow::{util::cli::{ConnectedDirect, ConnectedSink, ConnectedSource}, hydroflow_syntax};

use skypie_lib::{WriteChoice, Decision, skypie_lib::{read_choice::ReadChoice}};

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;
    
    println!("Starting candidate_policies_launch.rs");


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

    let flow = hydroflow_syntax!(
        input = source_stream(input_recv) -> map(|x| -> skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType {hydroflow::util::deserialize_from_bytes(x.unwrap()).unwrap()});
        // Create decisions
        process = input -> map(|x: WriteChoice| Decision{write_choice: x, read_choice: ReadChoice::default()});
        // Output decisions
        process -> map(|x: Decision| hydroflow::util::serialize_to_bytes(x)) -> dest_sink(output_send);
        //input -> map(|x:skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType| hydroflow::util::serialize_to_bytes(x)) -> dest_sink(output_send);
    );

    eprintln!("Launching candidate_policies_launch.rs");
    hydroflow::util::cli::launch_flow(flow).await;
}