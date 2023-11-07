use itertools::Itertools;
use pyo3::{prelude::*, types::PyList};
use rayon::{prelude::IntoParallelRefIterator,iter::ParallelIterator};
use std::collections::HashMap;

use skypie_lib::{
    identifier::Identifier, object_store::ObjectStore, read_choice::ReadChoice, ApplicationRegion, Decision, WriteChoice,
};

use super::{Optimizer, Workload};

#[pyclass]
#[derive(Debug)]
pub struct ProfitBasedOptimizer {
    object_stores: Vec<ObjectStore>,
    application_regions: Vec<ApplicationRegion>,
}

impl Optimizer for ProfitBasedOptimizer {
    fn _optimize(&self, workload: &Workload) -> (f64, Decision) {
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

        (cost, decision)
    }
}

#[pymethods]
impl ProfitBasedOptimizer {
    #[new]
    #[pyo3(signature = (network_file, object_store_file, object_stores_considered, application_regions_considered, latency_file_path = None, latency_slo = None))]
    pub fn new(
        network_file: &str,
        object_store_file: &str,
        object_stores_considered: Vec<&str>,
        application_regions_considered: HashMap<&str,u16>,
        latency_file_path: Option<&str>,
        latency_slo: Option<f64>
    ) -> Self {

        let loader = Self::load(network_file, object_store_file, object_stores_considered, application_regions_considered, latency_file_path, &latency_slo);

        Self {
            object_stores: loader.object_stores,
            application_regions: loader.app_regions,
        }
    }

    pub fn optimize(&self, workload: &Workload) -> (f64, i32) {
        let (cost, decision) = self._optimize(workload);

        (cost, decision.write_choice.object_stores.len() as i32)
    }

    pub fn optimize_batch<'py>(&self, workloads: &'py PyList) -> Vec<(f64, i32)> {
        
        let workloads = workloads.iter().map(|x|{
            let w = x.extract::<PyRef<'py,Workload>>().unwrap();
            (*w).clone()
        }).collect_vec();
        
        workloads.par_iter().map(|w| {self.optimize(&w)}).collect()
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self)
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
}
