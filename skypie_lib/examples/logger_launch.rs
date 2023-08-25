use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use clap::Parser;
use hydroflow::util::cli::{ConnectedDirect, ConnectedSource};
use hydroflow::util::deserialize_from_bytes;
use hydroflow::hydroflow_syntax;
use itertools::Itertools;
use skypie_lib::read_choice::ReadChoice;
use skypie_lib::{Args, Loader, Decision};
use skypie_lib::log_entry::SkyPieLogEntryType;

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

    let input_recv = ports
        .port("input")
        // connect to the port with a single recipient
        .connect::<ConnectedDirect>() 
        .await
        .into_source();

    type Input = (SkyPieLogEntryType, std::time::Duration);

    // Write basic stats to file
    let no_app_regions = loader.app_regions.len();
    let read_choice: ReadChoice = ReadChoice::new(no_app_regions);
    let no_app_regions = no_app_regions as i64;
    // Number of dimensions is the number of workload parameters/cost coefficients + 1 for the intercept
    let no_dimensions = Decision {
        write_choice: Default::default(),
        read_choice,
    }.plane_iter().len() as i64;

    let optimal_partitions: Vec<String> = if let Some(output_file_name) = args.output_file_name {
        let file_extension = output_file_name.extension().unwrap().to_str().unwrap();
        let file_name = output_file_name.file_stem().unwrap().to_str().unwrap();
        (0..args.redundancy_elimination_workers)
            .map(|i| format!("{}_{}.{}", file_name, i, file_extension))
            .collect_vec()
    } else {
        vec![]
    };
    let candidate_partitions: Vec<String> = if let Some(output_file_name) = args.output_candidates_file_name {
        let file_extension = output_file_name.extension().unwrap().to_str().unwrap();
        let file_name = output_file_name.file_stem().unwrap().to_str().unwrap();
        (0..args.redundancy_elimination_workers)
            .map(|i| format!("{}_{}.{}", file_name, i, file_extension))
            .collect_vec()
    } else {
        vec![]
    };

    let replication_factor = args.replication_factor as u64;

    let stats_file_name = format!("{}/stats", args.experiment_name);
    let mut stats = skypie_proto_messages::Wrapper::new(
        loader
            .object_stores
            .iter()
            .map(|o| format!("{}-{}", o.region.name, o.name))
            .collect_vec(),
        candidate_partitions,
        optimal_partitions,
        replication_factor,
        no_app_regions,
        no_dimensions
    );

    let flow = hydroflow_syntax! {

        input = source_stream(input_recv) -> map(|x| -> Input {deserialize_from_bytes(x.unwrap()).unwrap()});
        input -> fold::<'static>(Default::default(), |map: &mut HashMap::<SkyPieLogEntryType, Duration>, (entry_type, duration)|{
                *(map.entry(entry_type).or_default()) += duration;
            })
            -> for_each(|x: HashMap::<SkyPieLogEntryType, Duration>|{
                let zero_duration = Duration::from_secs(0);
                let total_time = x.get(&SkyPieLogEntryType::Total).unwrap_or(&zero_duration);
                let redundancy_elimination_time = x.get(&SkyPieLogEntryType::RedundancyElimination).unwrap_or(&zero_duration);
                let write_chioce_time = x.get(&SkyPieLogEntryType::WriteChoiceGeneration).unwrap_or(&zero_duration);
                let total_time = *total_time + *write_chioce_time;

                let enumerator_time = if total_time < *redundancy_elimination_time {
                    println!("Temporary inconsistency, total time is less than redundancy elimination time!");
                    Duration::default()
                } else {
                    total_time - (*redundancy_elimination_time)
                };
                println!("Total time: {:?}, Enumerator time: {:?}, Redundancy elimination time: {:?}, Write Choice time: {:?}", total_time, enumerator_time, redundancy_elimination_time, write_chioce_time);

                let partitioner_time_ns = redundancy_elimination_time.as_secs() as i64 * 1_000_000_000 + redundancy_elimination_time.subsec_nanos() as i64;

                let run = stats.tier_advise.as_mut().unwrap()
                    .replication_factor.entry(replication_factor).or_default()
                    .runs.entry("place_holder".to_string()).or_default();
                run.enumerator_time_ns = Some(enumerator_time.as_secs() as i64 * 1_000_000_000 + enumerator_time.subsec_nanos() as i64);
                run.partitioner_time_ns = Some(partitioner_time_ns);

                let optimizer = stats.tier_advise.as_mut().unwrap()
                    .replication_factor.entry(replication_factor).or_default()
                    .runs.entry("place_holder".to_string()).or_default()
                    .optimal_partitions_by_optimizer.entry("MosekOptimizerType.InteriorPoint_Clarkson_iter0_dsize1000".to_string()).or_default();
    
                optimizer.partitioner_computation_time_ns = Some(partitioner_time_ns);
                optimizer.partitioner_time_ns = Some(partitioner_time_ns);
                
                stats.save(&stats_file_name);
            });
    };

    println!("Launching");
    hydroflow::util::cli::launch_flow(flow).await;
    println!("Stopping");
}