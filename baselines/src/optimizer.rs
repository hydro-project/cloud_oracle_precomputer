use std::{collections::HashMap, path::PathBuf};
use skypie_lib::{Loader, Region, Decision, identifier::Identifier};

use crate::Workload;

pub(crate) trait Optimizer {
    fn load(
        network_file: &str,
        object_store_file: &str,
        object_stores_considered: Vec<&str>,
        application_regions_considered: HashMap<&str,u16>,
        latency_file_path: Option<&str>,
        latency_slo: &Option<f64>,
        verbose: Option<i32>
    ) -> Loader {

        let network_file = PathBuf::from(network_file);
        let object_store_file = PathBuf::from(object_store_file);
        let object_stores_considered = object_stores_considered.into_iter().map(|o| o.to_string()).collect::<Vec<_>>();
        let region_list = application_regions_considered.into_iter().map(|(name, id)| Region{id, name: name.to_string()}).collect::<Vec<_>>();
        let latency_file_path = latency_file_path.as_ref().map(|s| PathBuf::from(s));
        
        let loader = Loader::with_region_and_object_store_names(&network_file, &object_store_file, region_list, &object_stores_considered, &latency_file_path, latency_slo, verbose);
        
        loader
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

    fn _optimize(&self, workload: &Workload) -> (f64, Decision);
}