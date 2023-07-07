//use clap::Parser;

use std::collections::HashMap;

use hydroflow::util::cli::{ConnectedDirect, ConnectedSink};
use hydroflow::util::{serialize_to_bytes};
use hydroflow::hydroflow_syntax;
use skypie_lib::skypie_lib::network_record::NetworkCostMap;
use skypie_lib::skypie_lib::read_choice::ReadChoice;
use skypie_lib::{ApplicationRegion, Decision, Region, WriteChoice};
//use skypie_lib::Loader;
//use skypie_lib::skypie_lib::args::Args;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;
use skypie_lib::skypie_lib::object_store::{ObjectStore, Cost};

fn create_dummy_decisions() -> Vec<Decision> {
    let regions = vec![Region{id:0, name: "0".to_string()} ,Region{id: 0, name: "1".to_string()}];
        let egress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 1.0),
            (regions[1].clone(), 2.0)
        ]);
        let ingress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 0.0),
            (regions[1].clone(), 0.0)
        ]);

        let app_regions: Vec<ApplicationRegion> = regions.iter().map(|r|{ApplicationRegion{region: r.clone(), egress_cost: egress_cost.clone(), ingress_cost: ingress_cost.clone()}}).collect();
        
        let mut object_stores = vec![
            ObjectStore{id: 0, name: "0".to_string(), region: regions[0].clone(), cost: Cost { size_cost: 1.0, put_cost: 2.0, put_transfer: 4.0, get_cost: 3.0, get_transfer: 5.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }},
            ObjectStore{id: 1, name: "1".to_string(), region: regions[1].clone(), cost: Cost { size_cost: 10.0, put_cost: 20.0, put_transfer: 10.0, get_cost: 30.0, get_transfer: 20.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }}
        ];

        for  o in object_stores.iter_mut() {
            o.cost.add_egress_costs(egress_cost.clone());
            o.cost.add_ingress_costs(ingress_cost.clone());
        }

        let o0 = &object_stores[0];
        let o1 = &object_stores[1];
        let a0 = &app_regions[0];
        // Egress of o0 to a0 including get_transfer + Ingress of a0 from o0: 1.0 + 5.0 + 0.0
        assert_eq!(o0.get_egress_cost(a0), 6.0 );
        // Ingress of o0 from a0 including put_transfer + Egress of a0 from o0: 0.0 + 4.0 + 1.0
        assert_eq!(o0.get_ingress_cost(a0), 5.0 );

        let decisions = vec![
            Decision{ write_choice: WriteChoice{object_stores: vec![o0.clone()]},
                read_choice: ReadChoice::from_iter(vec![(app_regions[0].clone(), o0.clone())])},
            Decision{ write_choice: WriteChoice{object_stores: object_stores.clone()},
                read_choice: ReadChoice::from_iter(vec![(app_regions[0].clone(), o0.clone()), (app_regions[1].clone(), o1.clone())])}
        ];

    decisions
}

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;

    let num_decisions = 2000;
    
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

    type Output = skypie_lib::skypie_lib::candidate_policies_hydroflow::OutputType;

    let flow = hydroflow_syntax!{
        /* write_choices = source_iter(0..10000) -> map(|_|{Output::default()});
        decisions = write_choices -> map(|x|{
            // Create a decision ref
            type H<'a> = ReadChoiceRef<'a>;
            let read_choice = H::default();
            let dref = DecisionRef{
                write_choice: Box::new(x.write_choice),
                read_choice: read_choice
            };

            dref
        }) */
        // Generate dummy decisions and then take the first num_decisions via zip
        dummy_decisions = source_iter(create_dummy_decisions().into_iter().cycle());
        num_decisions = source_iter(0..num_decisions);
        decisions = zip() -> map(|(d,_)|{
            d
        });
        dummy_decisions -> [0]decisions;
        num_decisions -> [1]decisions;

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