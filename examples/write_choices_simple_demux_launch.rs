use std::{sync::atomic::{compiler_fence, Ordering::SeqCst}};

use clap::Parser;
use itertools::Itertools;
use rand::Rng;

use hydroflow::hydroflow_syntax;
use hydroflow::util::cli::{ConnectedDemux, ConnectedDirect, ConnectedSink};
use hydroflow::util::serialize_to_bytes;
use skypie_lib::{influx_logger::{InfluxLogger, InfluxLoggerConfig}, skypie_lib::{output::OutputWrapper, iter_stream_batches::iter_stream_batches, noop_logger::NoopLogger}};
use skypie_lib::skypie_lib::args::Args;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;
use skypie_lib::{Loader, SkyPieLogEntry};

struct IterWrapper {
    iter: itertools::Combinations<std::vec::IntoIter<u16>>,
}

impl IterWrapper {
    pub fn new(object_stores: Vec<u16>, n: usize) -> IterWrapper {
        IterWrapper { iter: object_stores.into_iter().combinations(n) }
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

    // Write basic stats to file
    let stats = OutputWrapper::new(loader.object_stores.clone(), vec![], vec![], args.replication_factor.clone() as u64);
    let stats_file_name = format!("{}/stats.json", args.experiment_name);
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
    let output_log_frequency = 10000;

    /* let logger = InfluxLogger::new(InfluxLoggerConfig {
        host: args.influx_host.unwrap(),
        port: 8086,
        database: "skypie".to_string(),
        measurement: "test".to_string(),
    }); */
    let logger = NoopLogger::new();
    let logger_sink = Box::pin(logger.into_sink::<SkyPieLogEntry>());

    let mut rng = rand::thread_rng();

    let object_store_ids = object_stores.iter().map(|x| x.id).collect::<Vec<_>>();

    let iter_batch_size = 2000; //args.batch_size*20;
    let iter = IterWrapper::new(object_store_ids, replication_factor);
    //let iter = object_stores.into_iter().map(|x| vec![x]);
    let combo_batches_stream = iter_stream_batches(iter, iter_batch_size);

    let flow = hydroflow_syntax! {
        write_choices = source_stream(combo_batches_stream)
        //-> inspect(|_|{println!(".");})
        -> demux(|v, var_args!(out,time)| {
            let now = std::time::Instant::now();
            compiler_fence(SeqCst);
            time.give(now);
            out.give(v);
        });
        // Distribute the write choices among instances of next stage
        serialized = write_choices[out] //-> map(|x| skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType{object_stores: x})
        //-> map(|x:skypie_lib::skypie_lib::candidate_policies_hydroflow::InputType| serialize_to_bytes(x))
        -> map(|x| serialize_to_bytes(x))
        -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Write choices:", Some(output_log_frequency));
        })
        // Round robin send to the next stage
        //-> enumerate()
        -> map(|x| (rng.gen_range(0..redundancy_elimination_workers), x))
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
