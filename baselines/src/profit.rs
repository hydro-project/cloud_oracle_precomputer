use pyo3::prelude::*;
use std::{collections::HashMap, path::PathBuf};

use skypie_lib::{
    identifier::Identifier, object_store::ObjectStore, read_choice::ReadChoice, ApplicationRegion, Decision, Loader, WriteChoice, Region,
};

use super::workload::Workload;

/* fn main() {
    let args = Args::parse();

    let loader = Loader::new(
        &args.network_file,
        &args.object_store_file,
        &args.region_selector,
        &args.object_store_selector,
    );

    // For testing consider all object stores and application regions
    let object_stores = loader.object_stores.into_iter().map(|o| format!("{}-{}", o.region.name, o.name)).collect::<Vec<_>>();
    let application_regions = loader.app_regions.into_iter().map(|a| a.region.name).collect::<Vec<_>>();

    // Print the object stores and application regions
    println!("Object stores:");
    for o in &object_stores {
        print!("{}, ", o);
    }
    println!();
    println!("Application regions:");
    for a in &application_regions {
        print!("{}, ", a);
    }
    println!();

    let workload = Workload {
        size: 1.0,
        puts: 0.0,
        gets: vec![1000.0; application_regions.len()],
        ingress: vec![0.0; application_regions.len()],
        egress: vec![1000.0; application_regions.len()],
    };

    let optimizer = ProfitBasedOptimizer::new(&args.network_file, &args.object_store_file, &args.region_selector, &args.object_store_selector, &object_stores, &application_regions);

    let (cost, decision) = optimizer.profit_based(&workload);
    println!("Cost: {}", cost);
    println!(
        "Object stores in decision: {:?}",
        decision.write_choice.object_stores.len()
    );
} */

#[pyclass]
pub struct ProfitBasedOptimizer {
    object_stores: Vec<ObjectStore>,
    application_regions: Vec<ApplicationRegion>,
}

#[pymethods]
impl ProfitBasedOptimizer {
    #[new]
    pub fn new(
        network_file: &str,
        object_store_file: &str,
        //region_selector: &str,
        //object_store_selector: &str,
        object_stores_considered: Vec<&str>,
        application_regions_considered: HashMap<&str,u16>,
    ) -> ProfitBasedOptimizer {

        //assert!(!object_stores_considered.is_empty());
        //assert!(!application_regions_considered.is_empty());

        let network_file = PathBuf::from(network_file);
        let object_store_file = PathBuf::from(object_store_file);
        let object_stores_considered = object_stores_considered.into_iter().map(|o| o.to_string()).collect::<Vec<_>>();
        let region_list = application_regions_considered.into_iter().map(|(name, id)| Region{id, name: name.to_string()}).collect::<Vec<_>>();
        
        let loader = Loader::with_region_and_object_store_names(&network_file, &object_store_file, region_list, &object_stores_considered);
        
        let object_stores = loader.object_stores;
        let application_regions = loader.app_regions;

        /* let loader = Loader::new(
            &network_file,
            &object_store_file,
            region_selector,
            object_store_selector,
        );

        let object_stores_considered_set =
            object_stores_considered.into_iter().collect::<HashSet<_>>();
        let application_regions_considered_set = application_regions_considered
            .into_iter()
            .collect::<HashSet<_>>();

        for o in loader.object_stores.iter() {
            println!("{}-{}", o.region.name, o.name);
        }

        for a in loader.app_regions.iter() {
            println!("{}", a.region.name);
        }

        let object_stores = loader
            .object_stores
            .into_iter()
            .filter(|o| {
                let f = format!("{}-{}", o.region.name, o.name);
                object_stores_considered_set.contains(f.as_str())
            })
            .collect::<Vec<_>>();
        let application_regions = loader
            .app_regions
            .into_iter()
            .filter(|a| application_regions_considered.contains_key(a.region.name.as_str()))
            .map(|mut a|{
                a.region.id = application_regions_considered.get(a.region.name.as_str()).unwrap().clone();
                a
            })
            .collect::<Vec<_>>(); */

        ProfitBasedOptimizer {
            object_stores,
            application_regions,
        }
    }

    pub fn optimize(&self, workload: &Workload) -> (f64, i32) /*(f64, Decision)*/ {
        let mut decision = self.initialize_decision(&workload);
        let mut cost = self.cost(&workload, &decision);

        #[cfg(dev)]
        println!(
            "Initial object store {}-{} with cost {}",
            decision.write_choice.object_stores[0].region.name,
            decision.write_choice.object_stores[0].name,
            cost
        );

        let mut object_stores = self.object_stores.clone();

        loop {
            let mut profit = 0.0;
            let mut object_stores_next = Vec::with_capacity(object_stores.len());

            // Search the object store that gives the highest profit when adding it to the current scheme
            for o in object_stores {
                let new_placement = self.add_object_store(&o, &decision, workload);
                let new_cost = self.cost(&workload, &new_placement);
                let profit_new = (cost - new_cost) / workload.size;

                #[cfg(dev)]
                println!(
                    "Profit of object store {}-{}: {}",
                    o.region.name, o.name, profit_new
                );

                if profit_new > profit {
                    profit = profit_new;
                    decision = new_placement;
                    cost = new_cost;
                } else if profit_new > 0.0 {
                    // Keep only object stores with positive profit, since profit is decreasing
                    object_stores_next.push(o);
                }
            }

            if profit <= 0.0 {
                break;
            }

            object_stores = object_stores_next;
            
            #[cfg(dev)]{
                let o = decision.write_choice.object_stores.last().unwrap();
                println!(
                    "Including object store {}-{} with profit {}",
                    o.region.name, o.name, profit
                );
            }
        }

        // XXX: Implement serialization of decision, but we don't care right now.
        //(cost, decision)
        (cost, decision.write_choice.object_stores.len() as i32)
    }
}

impl ProfitBasedOptimizer {
    // Object store with single cheapest object store
    fn initialize_decision(&self, workload: &Workload) -> Decision {
        let cheapest_object_store = self
            .object_stores
            .iter()
            .map(|object_store| {
                let mut total_cost = 0.0;

                total_cost += object_store.cost.size_cost * workload.size;
                total_cost += object_store.cost.put_cost * workload.puts;

                for app in self.application_regions.iter() {
                    total_cost += object_store.get_ingress_cost(app) * workload.get_ingress(app.get_id() as usize);
                    total_cost += object_store.compute_read_costs(
                        app,
                        workload.get_gets(app.get_id() as usize),
                        workload.get_egress(app.get_id() as usize),
                    );
                }

                (total_cost, object_store)
            })
            .min_by(|(cost1, _), (cost2, _)| cost1.partial_cmp(cost2).unwrap())
            .unwrap()
            .1;

        let write_choice = WriteChoice {
            object_stores: vec![cheapest_object_store.clone()],
        };
        let mut read_choice = ReadChoice::new(self.application_regions.len());
        for app in self.application_regions.iter() {
            read_choice.insert(app.clone(), cheapest_object_store.clone());
        }
        Decision {
            write_choice,
            read_choice,
        }
    }

    fn add_object_store(
        &self,
        new_object_store: &ObjectStore,
        decision: &Decision,
        workload: &Workload,
    ) -> Decision {
        // Copy the current placement decision and add the new object store

        let mut write_choice = decision.write_choice.clone();
        write_choice.object_stores.push(new_object_store.clone());
        let new_placement = Decision {
            write_choice,
            read_choice: decision
                .read_choice
                .iter()
                .map(|(app, object_store)| {
                    if object_store == new_object_store {
                        // Simply copy when object store is identical
                        (app.clone(), new_object_store.clone())
                    } else {
                        // Check if new object store has lower cost
                        let cost_o = new_object_store.compute_read_costs(
                            app,
                            workload.get_gets(app.get_id() as usize),
                            workload.get_egress(app.get_id() as usize),
                        );
                        let cost_prev = object_store.compute_read_costs(
                            app,
                            workload.get_gets(app.get_id() as usize),
                            workload.get_egress(app.get_id() as usize),
                        );

                        if cost_o < cost_prev {
                            #[cfg(dev)]
                            println!(
                                "Taking new object store {}-{} for app {}",
                                new_object_store.region.name,
                                new_object_store.name,
                                app.region.name
                            );
                            (app.clone(), new_object_store.clone())
                        } else {
                            (app.clone(), object_store.clone())
                        }
                    }
                })
                .collect(),
        };

        return new_placement;
    }

    fn cost(&self, workload: &Workload, placement: &Decision) -> f64 {
        /*
        Cost of the workload under the given placement
        */
        let mut total_cost = 0.0;

        for object_store in placement.write_choice.object_stores.iter() {
            total_cost += object_store.cost.size_cost * workload.size;
            total_cost += object_store.cost.put_cost * workload.puts;
        }

        for (app, object_store) in placement.read_choice.iter() {
            total_cost += object_store.get_ingress_cost(app) * workload.get_ingress(app.get_id() as usize);
            total_cost += object_store.compute_read_costs(
                app,
                workload.get_gets(app.get_id() as usize),
                workload.get_egress(app.get_id() as usize),
            );
        }

        total_cost
    }
}
