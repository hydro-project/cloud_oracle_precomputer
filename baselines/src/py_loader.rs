use pyo3::prelude::*;
use std::collections::HashMap;
use skypie_lib::Loader;
use std::path::PathBuf;

use skypie_lib::{
    object_store::ObjectStore, ApplicationRegion, Decision
};

use super::{Optimizer, Workload};

#[pyclass]
#[derive(Debug)]
pub struct PyLoader {
    object_stores: Vec<ObjectStore>,
    application_regions: Vec<ApplicationRegion>,
    #[pyo3(get)]
    pub network_latency: HashMap<String, HashMap<String, f64>>
}

impl Optimizer for PyLoader {
    fn _optimize(&self, _workload: &Workload) -> (f64, Decision) {
        panic!("Not implemented");
    }
}

#[pymethods]
impl PyLoader {
    #[new]
    #[pyo3(signature = (network_file, object_store_file, object_stores_considered, application_regions_considered, latency_file_path = None, latency_slo = None, verbose = None, region_selector = None, object_store_selector = None))]
    pub fn new(
        network_file: &str,
        object_store_file: &str,
        object_stores_considered: Vec<&str>,
        application_regions_considered: HashMap<&str,u16>,
        latency_file_path: Option<&str>,
        latency_slo: Option<f64>,
        verbose: Option<i32>,
        region_selector: Option<&str>,
        object_store_selector: Option<&str>
    ) -> Self {

        let loader = if object_stores_considered.is_empty() && application_regions_considered.is_empty() && region_selector.is_some() && object_store_selector.is_some() {
            println!("No object stores or application regions considered. Falling back to all object stores and application regions.");

            let network_file = PathBuf::from(network_file);
            let object_store_file = PathBuf::from(object_store_file);
            let latency_file_path = latency_file_path.as_ref().map(|s| PathBuf::from(s));
            let region_selector = region_selector.unwrap();
            let object_store_selector = object_store_selector.unwrap();
            
            let loader = Loader::new(&network_file, &object_store_file, &region_selector, &object_store_selector, &latency_file_path, &latency_slo, verbose);            
            loader
        } else {
            let loader = Self::load(network_file, object_store_file, object_stores_considered, application_regions_considered, latency_file_path, &latency_slo, verbose);
            loader
        };

        let network_latency = loader.network_latency.iter()
            .map(|(r, l)| (r.name.clone(), l.iter().map(|(r, l)| (r.name.clone(), *l)).collect::<HashMap<_,_>>()))
            .collect::<HashMap<_,_>>();

        Self {
            object_stores: loader.object_stores,
            application_regions: loader.app_regions,
            network_latency: network_latency
        }
    }

    pub fn get_price(&self) -> HashMap<String, f64> {
        self.object_stores.iter().map(|o| (o.fully_qualified_name(), o.cost.get_cost)).collect::<HashMap<_,_>>()
    }
    
    pub fn put_price(&self) -> HashMap<String, f64> {
        self.object_stores.iter().map(|o| (o.fully_qualified_name(), o.cost.put_cost)).collect::<HashMap<_,_>>()
    }

    pub fn storage_price(&self) -> HashMap<String, f64> {
        self.object_stores.iter().map(|o| (o.fully_qualified_name(), o.cost.size_cost)).collect::<HashMap<_,_>>()
    }

    pub fn network_price(&self) -> HashMap<String, HashMap<String, f64>> {
        let mut network_costs = HashMap::new();
        for source in &self.object_stores {
            // Network transfer costs from object store to application region
            for destination in &self.application_regions {
                let egress_cost = source.get_egress_cost(destination);
                let ingress_cost = source.get_ingress_cost(destination);

                let source_name = source.fully_qualified_name();
                let destination_name = destination.region.name.clone();
                // Network egress from object store to application region
                network_costs.entry(source_name.clone()).or_insert_with(HashMap::new).insert(destination_name.clone(), egress_cost);
                // Network ingress from application region to object store
                network_costs.entry(destination_name).or_insert_with(HashMap::new).insert(source_name, ingress_cost);
            }
            // Network transfer costs from object store to object store (for migration)
            for destination in &self.object_stores {
                let migration_cost = source.get_transfer_cost(destination);

                let source_name = source.fully_qualified_name();
                let destination_name = destination.fully_qualified_name();
                // Network egress from object store to object store
                network_costs.entry(source_name.clone()).or_insert_with(HashMap::new).insert(destination_name.clone(), migration_cost);
            }
        }

        network_costs
    }

    pub fn application_region_names(&self) -> Vec<String> {
        self.application_regions.iter().map(|r| r.region.name.clone()).collect::<Vec<_>>()
    }

    pub fn application_region_mapping(&self) -> HashMap<String, u64> {
        self.application_regions.iter().map(|r| (r.region.name.clone(), r.region.id as u64)).collect::<HashMap<_,_>>()
    }

    pub fn object_store_names(&self) -> Vec<String> {
        self.object_stores.iter().map(|o| o.fully_qualified_name()).collect::<Vec<_>>()
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}