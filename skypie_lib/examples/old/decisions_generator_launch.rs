//use clap::Parser;

use std::collections::HashMap;

use clap::Parser;
use hydroflow::util::cli::{ConnectedDirect, ConnectedSink};
use hydroflow::util::{serialize_to_bytes};
use hydroflow::hydroflow_syntax;
use itertools::Itertools;
use skypie_lib::iter_stream_batches::iter_stream_batches;
use skypie_lib::network_record::NetworkCostMap;
use skypie_lib::read_choice::ReadChoice;
use skypie_lib::{ApplicationRegion, Decision, Region, WriteChoice, Args};
//use skypie_lib::Loader;
//use skypie_lib::skypie_lib::args::Args;
use skypie_lib::monitor::MonitorMovingAverage;
use skypie_lib::object_store::{ObjectStore, Cost, ObjectStoreStruct};

fn create_dummy_decisions(num_decisions: u16) -> Vec<Decision> {
    let regions = vec![Region{id:0, name: "0".to_string()} ,Region{id: 0, name: "1".to_string()}];
        let egress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 1.0),
            (regions[1].clone(), 2.0)
        ]);
        let ingress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 0.0),
            (regions[1].clone(), 0.0)
        ]);

        let app_regions = regions.iter().map(|r|{ApplicationRegion{region: r.clone(), egress_cost: egress_cost.clone(), ingress_cost: ingress_cost.clone()}}).collect_vec();
        
        let o0 = ObjectStore::new(ObjectStoreStruct{id: 0, name: "0".to_string(), region: regions[0].clone(), cost: Cost { size_cost: 1.0, put_cost: 2.0, put_transfer: 4.0, get_cost: 3.0, get_transfer: 5.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }});

        let mut object_stores: Vec<ObjectStore> = (1..num_decisions).into_iter().map(|i|{
            let mut cost = o0.cost.clone();
            cost.size_cost = cost.size_cost + i as f64;

            ObjectStore::new(ObjectStoreStruct{id: i, name: i.to_string(), region: regions[0].clone(), cost: cost})
        }).collect_vec();

        for  o in object_stores.iter_mut() {
            o.cost.add_egress_costs(egress_cost.clone());
            o.cost.add_ingress_costs(ingress_cost.clone());
        }

        let decisions: Vec<Decision> = object_stores.into_iter().map(|o|{
            Decision{ write_choice: WriteChoice{object_stores: vec![o.clone()]},
                read_choice: ReadChoice::from_iter(vec![(app_regions[0].clone(), o.clone()), (app_regions[1].clone(), o.clone())])}
        }).collect_vec();

    decisions
}

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;

    let args = Args::parse();

    let num_decisions = 10000;
    let num_dummy_decisions = args.batch_size;
    
    // Get ports
    let output_send = ports
        .port("output")
        .connect::<ConnectedDirect>() 
        .await
        .into_sink();

    //let regions = loader.regions;
    //let object_stores = loader.object_stores;
    //let replication_factor = args.replication_factor;
    //let batch_size = args.batch_size;

    let mut output_monitor = MonitorMovingAverage::new(1000);

    type Output = skypie_lib::candidate_policies_hydroflow::OutputType;

    let iter_batch_size = 10;
    let combo_batches_stream = iter_stream_batches(create_dummy_decisions(num_dummy_decisions as u16).into_iter().cycle().take(num_decisions), iter_batch_size);

    //

    let flow = hydroflow_syntax!{
        // Generate dummy decisions and then take the first num_decisions via zip
        /* cycle_decisions = source_iter(create_dummy_decisions().into_iter().cycle());
        num_decisions = source_iter(0..num_decisions);
        decisions = zip() -> map(|(d,_)|{
            d
        });
        cycle_decisions -> [0]decisions;
        num_decisions -> [1]decisions; */
        decisions = source_stream(combo_batches_stream);

        decisions -> map(|d: Output|{
            let b = serialize_to_bytes(d);
            b
        })
        -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Decisions:", Some(1000));
        })
        -> dest_sink(output_send);
    };

    println!("Generating {} decisions", num_decisions);
    hydroflow::util::cli::launch_flow(flow).await;
    println!("DONE: write choices sent");
}