use std::{sync::atomic::{compiler_fence, Ordering::SeqCst}};

use clap::Parser;
use itertools::Itertools;

use hydroflow::hydroflow_syntax;
use hydroflow::util::cli::{ConnectedDemux, ConnectedDirect, ConnectedSink};
use hydroflow::util::serialize_to_bytes;
use skypie_lib::{influx_logger::{InfluxLogger, InfluxLoggerConfig}, skypie_lib::output::OutputWrapper};
use skypie_lib::skypie_lib::args::Args;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;
use skypie_lib::{Loader, SkyPieLogEntry};

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

    // Write basic stats to file
    let stats = OutputWrapper::new(loader.object_stores.clone(), vec![], vec![], args.replication_factor.clone() as u64);
    let stats_file_name = format!("{}_stats.json", args.experiment_name);
    stats.save_json(&stats_file_name);

    // Get ports
    let output_send = ports
        .port("output")
        .connect::<ConnectedDemux<ConnectedDirect>>()
        .await
        .into_sink();

    //let regions = loader.regions;
    let object_stores = loader.object_stores;
    // XXX: For debugging
    //let object_stores = object_stores.into_iter().take(10).collect::<Vec<_>>();

    let replication_factor = args.replication_factor;
    let redundancy_elimination_workers: u32 = args.redundancy_elimination_workers;

    let mut output_monitor = MonitorMovingAverage::new(1000);
    let output_log_frequency = 100;

    let logger = InfluxLogger::new(InfluxLoggerConfig {
        host: "localhost".to_string(),
        port: 8086,
        database: "skypie".to_string(),
        measurement: "test".to_string(),
    });
    let logger_sink = Box::pin(logger.into_sink::<SkyPieLogEntry>());

    let flow = hydroflow_syntax! {
        write_choices = source_iter(object_stores.into_iter().combinations(replication_factor))
        -> demux(|v, var_args!(out,time)| {
            let now = std::time::Instant::now();
            compiler_fence(SeqCst);
            time.give(now);
            out.give(v);
        });
        // Distribute the write choices among instances of next stage
        serialized = write_choices[out] -> map(|x| skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType{object_stores: x})
        -> map(|x:skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType| serialize_to_bytes(x))
        -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Write choices:", Some(output_log_frequency));
        })
        // Round robin send to the next stage
        -> enumerate()
        -> map(|(i, x)| (i % redundancy_elimination_workers, x))
        -> demux(|v, var_args!(out, time)|{
            let now = std::time::Instant::now();
            compiler_fence(SeqCst);
            time.give((1, now));
            out.give(v);
        });

        serialized[out] -> dest_sink(output_send);

        write_choices[time] -> [0]measurement;
        serialized[time] -> map(|_|{1}) -> reduce::<'tick>(|acc: &mut u64, i|{*acc = *acc + i;}) -> [1]measurement;
        measurement = zip() -> map(|(start_time, count)|(start_time.elapsed().as_secs_f64(), count))
        -> map(|(cycle_time, count)|{
            SkyPieLogEntry::new(cycle_time,count,"write_choices".to_string(), args.experiment_name.clone())
        })
        -> dest_sink(logger_sink);

    };

    hydroflow::util::cli::launch_flow(flow).await;
    println!("DONE: write choices sent");
}
