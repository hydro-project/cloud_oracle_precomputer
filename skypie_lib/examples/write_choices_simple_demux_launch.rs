use clap::Parser;
use itertools::Itertools;

use hydroflow::hydroflow_syntax;
use hydroflow::util::cli::{ConnectedDemux, ConnectedDirect, ConnectedSink};
use hydroflow::util::serialize_to_bytes;
use skypie_lib::args::Args;
use skypie_lib::log_entry::SkyPieLogEntryType;
use skypie_lib::monitor::MonitorMovingAverage;
use skypie_lib::iter_stream_batches::iter_stream_batches;
use skypie_lib::Loader;

struct IterWrapper {
    iter: itertools::Combinations<std::vec::IntoIter<u16>>,
}

impl IterWrapper {
    pub fn new(object_stores: Vec<u16>, n: usize) -> IterWrapper {
        IterWrapper {
            iter: object_stores.into_iter().combinations(n),
        }
    }
}

impl Iterator for IterWrapper {
    type Item = Vec<u16>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;

    // Load the input
    let args = Args::parse();

    let loader = Loader::new(
        &args.network_file,
        &args.object_store_file,
        &args.region_selector,
    );

    // Get ports
    let output_send = ports
        .port("output")
        .connect::<ConnectedDemux<ConnectedDirect>>()
        .await
        .into_sink();

    let time_sink = ports
        .port("time_output")
        .connect::<ConnectedDirect>() 
        .await
        .into_sink();

    let object_stores = loader.object_stores;
    let replication_factor = args.replication_factor;
    let redundancy_elimination_workers: u32 = args.redundancy_elimination_workers;

    let mut output_monitor = MonitorMovingAverage::new(1000);
    let output_log_frequency = 10000;

    let object_store_ids = object_stores.iter().map(|x| x.id).collect::<Vec<_>>();

    let iter_batch_size = 20; //args.batch_size*20;
    let iter = IterWrapper::new(object_store_ids, replication_factor);
    let combo_batches_stream = iter_stream_batches(iter, iter_batch_size);

    let flow = hydroflow_syntax! {
        write_choices = source_stream(combo_batches_stream);
        // Distribute the write choices among instances of next stage
        serialized = write_choices
            -> map(|x| serialize_to_bytes(x))
            -> inspect(|_|{
                output_monitor.add_arrival_time_now();
                output_monitor.print("Write choices:", Some(output_log_frequency));
            })
            // Round robin send to the next stage
            -> map(|x| -> (u32, _) {(context.current_tick() as u32 % redundancy_elimination_workers, x)})
            -> tee();
            //-> map(|x| (rng.gen_range(0..redundancy_elimination_workers), x));

        serialized -> dest_sink(output_send);

        // Measure the total cycle time here
        tick_duration =
            serialized -> reduce(|_,_| ()) // Current tick
            -> map(|_| context.current_tick_start()) // Time of current tick
            -> defer_tick() // Wait for next tick
            // Duration between start of current tick and start of next tick
            -> map(|prev_tick| context.current_tick_start() - prev_tick)
            -> map(|d|(SkyPieLogEntryType::WriteChoiceGeneration, d));

        tick_duration -> map(|d|{serialize_to_bytes(d)}) -> dest_sink(time_sink);

    };

    hydroflow::util::cli::launch_flow(flow).await;
    println!("DONE: write choices sent");
}
