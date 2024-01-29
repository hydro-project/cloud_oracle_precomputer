use std::collections::HashMap;

use hydroflow::serde_json;
use serde::Serialize;

use super::object_store::ObjectStore;

#[derive(Debug, Serialize)]
pub struct OutputDecision {
    // Name of field in serialization
    #[serde(rename = "replicationScheme")]
    pub replication_scheme: OutputScheme,
    #[serde(rename = "costWLHalfplane")]
    pub cost_wl_halfplane: Vec<f64>,
    // Seconds with unix epoch
    pub timestamp: u64
}

#[derive(Debug, Serialize)]
pub struct OutputScheme {
    #[serde(rename = "objectStores")]
    pub object_stores: Vec<String>, // Only output region and name of object stores, skipping former map of object stores to output cost, etc.
    #[serde(rename = "appAssignments")]
    pub app_assignments: Vec<OutputAssignment>,
    //cost: ...
}

#[derive(Debug, Serialize)]
pub struct OutputAssignment {
    pub app: String,
    #[serde(rename = "objectStore")]
    pub object_store: String,
}

#[derive(Debug, Serialize)]
pub struct OutputWrapper {
    //pub args
    pub tier_advise: OutputTierAdvise,
}

impl OutputWrapper {
    pub fn new(object_stores: Vec<ObjectStore>, candidate_partitions: Vec<OutputDecision>, optimal_partitions: Vec<OutputDecision>, replication_factor: u64) -> Self {

        //let replication_factor = 0;
        let max_replication_factor = replication_factor;
        let min_replication_factor = replication_factor;


        let run = OutputRun::new(max_replication_factor, min_replication_factor, object_stores, candidate_partitions, optimal_partitions);

        let tier_advise = OutputTierAdvise::new(replication_factor, run);

        Self {
            tier_advise
        }
    }

    pub fn save_json(&self, path: &str) {
        let file = std::fs::File::create(path).unwrap();
        serde_json::to_writer(file, &self).unwrap();
    }
}

#[derive(Debug, Serialize)]
pub struct OutputTierAdvise {
    pub replication_factor: HashMap<u64, HashMap<String, OutputRun>>,
}

impl OutputTierAdvise {
    pub fn new(replication_factor: u64, run: OutputRun) -> Self {

        let run_name = "place_holder".to_string();
        let run_map = HashMap::from_iter(vec![(run_name, run)]);
        let replication_factor = HashMap::from_iter(vec![(replication_factor, run_map )]);
        Self {
            replication_factor
        }
    }
}

#[derive(Debug, Serialize)]
pub struct OutputRun {
    //pub avg_degeneracy: i64,
    pub enumerator_time_ns: i64,
    //pub max_degeneracy: i64,
    pub max_replication_factor: u64,
    pub min_replication_factor: u64,
    pub no_app_regions: i64,
    //pub no_degeneracies: i64,
    pub no_dimensions: i64,
    pub no_facets: i64,
    pub no_object_stores: i64,
    //pub no_redundant_facets: i64,
    //pub no_replication_schemes: i64,
    //pub no_ridges: i64,
    //pub no_vertices: i64,
    pub partitioner_time_ns: i64,
    pub object_stores_considered: Vec<String>,
    pub candidate_partitions: Vec<OutputDecision>,
    pub optimal_partitions_by_optimizer: HashMap<String, OutputOptimalByOptimizer>,
}

impl OutputRun {
    pub fn new(max_replication_factor: u64, min_replication_factor: u64, object_stores: Vec<ObjectStore>, candidate_partitions: Vec<OutputDecision>, optimal_partitions: Vec<OutputDecision>) -> Self {

        let no_app_regions = if candidate_partitions.len() > 0{
            candidate_partitions.first().unwrap().replication_scheme.app_assignments.len() as i64
        } 
        else if optimal_partitions.len() > 0 {
            optimal_partitions.first().unwrap().replication_scheme.app_assignments.len() as i64
        }
        else {
            0
        };
        let no_dimensions = if candidate_partitions.len() > 0{
            candidate_partitions.first().unwrap().cost_wl_halfplane.len() as i64
        }
        else if optimal_partitions.len() > 0 {
            optimal_partitions.first().unwrap().cost_wl_halfplane.len() as i64
        } 
        else {
            0
        };
        let no_object_stores = object_stores.len() as i64;
        let no_facets = optimal_partitions.len() as i64;
        let object_stores_considered = object_stores.into_iter().map(|os| format!("{}-{}", os.region.name, os.name)).collect();
        //let optimal_partitions_by_optimizer = HashMap::from_iter(vec![("MosekOptimizerType.InteriorPoint_Clarkson_iter0_dsize1000".into(), OutputOptimalByOptimizer::new(optimal_partitions))]);
        let optimal_partitions_by_optimizer = HashMap::from_iter(vec![("MosekOptimizerType.InteriorPoint_Clarkson_iter0_dsize1000".into(), OutputOptimalByOptimizer::new(optimal_partitions))]);
        Self {
            enumerator_time_ns: 0,
            max_replication_factor,
            min_replication_factor,
            no_app_regions,
            no_dimensions,
            no_facets,
            no_object_stores,
            partitioner_time_ns: 0,
            object_stores_considered,
            candidate_partitions,
            optimal_partitions_by_optimizer,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct OutputOptimalByOptimizer {
    pub optimizer_type: String,
    pub optimal_partitions: Vec<OutputDecision>,
    pub no_facets: usize,
    pub partitioner_time_ns: i64,
    pub partitioner_computation_time_ns: i64,
    //pub no_redundant_facets: i64
}

impl OutputOptimalByOptimizer {
    pub fn new(optimal_partitions: Vec<OutputDecision>) -> Self {
        let partitioner_time_ns = 0;
        let partitioner_computation_time_ns = 0;

        let optimizer_type = r#"
        {
            "type": "intpnt",
            "useClarkson": true,
            "useGPU": false,
            "name": "MosekOptimizerType.InteriorPoint_Clarkson_iter0_dsize1000",
            "implementation": 1,
            "implementationArgs": {
                "device_query": "cuda:1",
                "device_check": "cuda:1"
            },
            "iteration": 0,
            "dsize": 1000,
            "strictReplication": true
        }
        "#
        .to_string();

        Self {
            optimizer_type,
            no_facets: optimal_partitions.len(),
            optimal_partitions,
            partitioner_time_ns,
            partitioner_computation_time_ns
        }
    }
}
