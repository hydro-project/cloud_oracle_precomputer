pub mod protobuf_file_sink;

use std::{collections::HashMap, path::Path};

use hydroflow::futures::SinkExt;
use prost::Message;
pub use protobuf_file_sink::ProtobufFileSink;

mod messages {
    include!(concat!(env!("OUT_DIR"), "/skypie.rs"));
}

pub use messages::{Assignment, Scheme, Decision, OptimalByOptimizer, Run, TierAdvise, Setting, Wrapper};

impl Wrapper {
    pub fn new(
        object_stores: Vec<String>,
        candidate_partitions: Vec<Decision>,
        optimal_partitions: Vec<Decision>,
        replication_factor: u64,
    ) -> Self {
        let max_replication_factor = replication_factor;
        let min_replication_factor = replication_factor;

        let run = Run::new(
            max_replication_factor,
            min_replication_factor,
            object_stores,
            candidate_partitions,
            optimal_partitions,
        );

        let tier_advise = Some(TierAdvise::new(replication_factor, run));

        Self { tier_advise }
    }

    /// Saves the protobuf message to a binary file at the given path.
    /// 
    /// # Arguments
    /// 
    /// * `path` - A string slice that holds the path where the binary file will be saved.
    /// 
    pub fn save(&self, path: &str) {
        let file_name = format!("{}.bin", path);
        let file_name = Path::new(&file_name);
        let capa = self.encoded_len()+42;
        let mut sink = ProtobufFileSink::new(file_name, capa, 0).unwrap();
        sink.start_send_unpin(self.clone()).unwrap();
        sink.flush().unwrap();
        let _: hydroflow::futures::sink::Close<'_, ProtobufFileSink, Self> = sink.close();
    }
}

impl TierAdvise {
    pub fn new(replication_factor: u64, run: Run) -> Self {
        let run_name = "place_holder".to_string();
        let setting = Setting {
            runs: HashMap::from_iter(vec![(run_name, run)]),
        };
    
        let replication_factor = HashMap::from_iter(vec![(replication_factor, setting)]);
        Self { replication_factor }
    }
}

impl Run {
    pub fn new(
        max_replication_factor: u64,
        min_replication_factor: u64,
        object_stores: Vec<String>,
        candidate_partitions: Vec<Decision>,
        optimal_partitions: Vec<Decision>,
    ) -> Run {
        let no_app_regions = if candidate_partitions.len() > 0 {
            candidate_partitions
                .first()
                .unwrap()
                .replication_scheme
                .as_ref()
                .unwrap()
                .app_assignments
                .len() as i64
        } else if optimal_partitions.len() > 0 {
            optimal_partitions
                .first()
                .unwrap()
                .replication_scheme
                .as_ref()
                .unwrap()
                .app_assignments
                .len() as i64
        } else {
            0
        };
        let no_app_regions = Some(no_app_regions);
    
        let no_dimensions = if candidate_partitions.len() > 0 {
            candidate_partitions
                .first()
                .unwrap()
                .cost_wl_halfplane
                .len() as i64
        } else if optimal_partitions.len() > 0 {
            optimal_partitions.first().unwrap().cost_wl_halfplane.len() as i64
        } else {
            0
        };
        let no_dimensions = Some(no_dimensions);
    
        let no_object_stores = Some(object_stores.len() as i64);
        let no_facets = Some(optimal_partitions.len() as i64);
        let object_stores_considered = object_stores;
            /* .into_iter()
            .map(|os| format!("{}-{}", os.region.name, os.name))
            .collect(); */
    
        let optimal_partitions_by_optimizer = HashMap::from_iter(vec![(
            "MosekOptimizerType.InteriorPoint_Clarkson_iter0_dsize1000".into(),
            OptimalByOptimizer::new(optimal_partitions),
        )]);
    
        Run {
            enumerator_time_ns: Some(0),
            max_replication_factor: Some(max_replication_factor),
            min_replication_factor: Some(min_replication_factor),
            no_app_regions,
            no_dimensions,
            no_facets,
            no_object_stores,
            partitioner_time_ns: Some(0),
            object_stores_considered,
            candidate_partitions,
            optimal_partitions_by_optimizer,
        }
    }
}

impl OptimalByOptimizer {
    pub fn new(optimal_partitions: Vec<Decision>) -> OptimalByOptimizer {
        let partitioner_time_ns = Some(0);
        let partitioner_computation_time_ns = Some(0);
    
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
        let optimizer_type = Some(optimizer_type);
    
        OptimalByOptimizer {
            optimizer_type,
            no_facets: Some(optimal_partitions.len() as i64),
            optimal_partitions,
            partitioner_time_ns,
            partitioner_computation_time_ns,
        }
    }
}