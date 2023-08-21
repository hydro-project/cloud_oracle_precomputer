use hydroflow::util::cli::{ConnectedDirect, ConnectedSink, ConnectedSource};
use hydroflow::util::{deserialize_from_bytes, serialize_to_bytes};
use hydroflow::hydroflow_syntax;

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

    let output_send = ports
        .port("output")
        .connect::<ConnectedDirect>() 
        .await
        .into_sink();

    hydroflow::util::cli::launch_flow(hydroflow_syntax! {
        
        output = union() -> dest_sink(output_send);
        source_iter(["hello".to_string()]) -> map(|x: String| serialize_to_bytes(x)) -> output;
        input = source_stream(input_recv) -> map(|x| deserialize_from_bytes(x.unwrap()).unwrap());
        input -> map(|x:String| serialize_to_bytes(x)) -> output;
    }).await;
}