pub mod protobuf_file_reader;
pub mod protobuf_file_sink;

pub use protobuf_file_reader::ProtobufFileReader;
pub use protobuf_file_sink::ProtobufFileSink;

#[allow(non_snake_case)]
mod messages {
    use crate::{ProtobufFileReader, ProtobufFileSink};
    use hydroflow::futures::SinkExt;
    use prost::Message;
    #[cfg(feature = "python-module")]
    use pyo3::prelude::*;
    use rayon::prelude::*;
    use std::{collections::HashMap, path::Path};

    include!(concat!(env!("OUT_DIR"), "/skypie.rs"));

    impl Wrapper {
        pub fn new(
            object_stores: Vec<String>,
            candidate_partitions: Vec<String>,
            optimal_partitions: Vec<String>,
            replication_factor: u64,
            no_app_regions: i64,
            no_dimensions: i64,
            optimizer_name: String,
            optimizer_type: String
        ) -> Self {
            let max_replication_factor = replication_factor;
            let min_replication_factor = replication_factor;

            let run = Run::new(
                max_replication_factor,
                min_replication_factor,
                object_stores,
                candidate_partitions,
                optimal_partitions,
                no_app_regions,
                no_dimensions,
                optimizer_name,
                optimizer_type
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
            let file_name = format!("{}.proto.bin", path);
            let file_name = Path::new(&file_name);
            let capa = self.encoded_len() + 42;
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
            candidate_partitions: Vec<String>,
            optimal_partitions: Vec<String>,
            no_app_regions: i64,
            no_dimensions: i64,
            optimizer_name: String,
            optimizer_type: String
        ) -> Run {
            let no_app_regions = Some(no_app_regions);

            let no_dimensions = Some(no_dimensions);

            let no_object_stores = Some(object_stores.len() as i64);
            let no_facets = Some(optimal_partitions.len() as i64);
            let object_stores_considered = object_stores;

            let optimal_partitions_by_optimizer = HashMap::from_iter(vec![(
                optimizer_name.into(),
                OptimalByOptimizer::new(optimal_partitions, optimizer_type),
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
        pub fn new(optimal_partitions: Vec<String>, optimizer_type: String) -> OptimalByOptimizer {
            let partitioner_time_ns = Some(0);
            let partitioner_computation_time_ns = Some(0);

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

    impl Assignment {
        pub fn compact(mut self, is_first: bool) -> Assignment {
            self.object_store = String::new();
            if !is_first {
                self.app = String::new();
            }
            self
        }
    }

    impl Scheme {
        pub fn compact(mut self, is_first: bool) -> Scheme {
            self.object_stores = Vec::with_capacity(0);
            self.app_assignments = self.app_assignments.into_iter().map(|x| x.compact(is_first)).collect();
            self
        }
    }

    impl Decision {
        pub fn compact(mut self, is_first: bool) -> Decision {
            if let Some(replication_scheme) = self.replication_scheme {
                self.replication_scheme = Some(replication_scheme.compact(is_first));
            }

            self
        }
    }

    pub fn load_decisions(paths: Vec<&Path>, compact: bool) -> Vec<Decision> {
        type M = Decision;

        // Treat first message compaction differently
        let mut is_first = true;
        let mut decisions = Vec::new();
        for path in paths {
            let message_iter = ProtobufFileReader::new(path).unwrap().into_iter_all::<M>();

            if compact {
                // Replace non-essential contend with empty values
                let compact_message_iter = message_iter.map(|x: M| {
                    let res = x.compact(is_first);
                    is_first = false;
                    res
                });
                decisions.extend(compact_message_iter);

            } else {
                decisions.extend(message_iter);
            }
        }

        decisions
    }

    pub fn load_decisions_parallel(paths: Vec<&Path>, threads: usize, compact: bool) -> Vec<Decision> {

        paths.chunks(paths.len() / threads).into_iter().par_bridge()
            .map(|chunk|load_decisions(chunk.to_vec(), compact))
            .reduce(|| vec![], |mut acc: Vec<Decision>, next| {acc.extend(next); acc})
    }

    pub fn load_wrapper(path: &Path) -> Wrapper {
        type M = Wrapper;

        let mut reader = ProtobufFileReader::new(path).unwrap();

        reader.read_next::<M>().unwrap()
    }
}

#[cfg(feature = "python-module")]
mod python {
    use crate::messages::{
        load_decisions, load_decisions_parallel, load_wrapper, Assignment, Decision, OptimalByOptimizer, Run, Scheme,
        Setting, TierAdvise, Wrapper,
    };
    use pyo3::prelude::*;
    use pyo3::pymethods;
    use pyo3::wrap_pyfunction;
    use std::path::Path;

    #[pymethods]
    impl Assignment {
        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }

        fn __str__(&self) -> String {
            self.__repr__()
        }
    }

    #[pymethods]
    impl Decision {
        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }

        fn __str__(&self) -> String {
            self.__repr__()
        }
    }

    #[pymethods]
    impl OptimalByOptimizer {
        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }

        fn __str__(&self) -> String {
            self.__repr__()
        }
    }

    #[pymethods]
    impl Run {
        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }

        fn __str__(&self) -> String {
            self.__repr__()
        }
    }

    #[pymethods]
    impl Scheme {
        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }

        fn __str__(&self) -> String {
            self.__repr__()
        }
    }

    #[pymethods]
    impl Setting {
        fn __repr__(&self) -> String {
            format!("{:?}", self.runs)
        }
    }

    #[pymethods]
    impl TierAdvise {
        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }

        fn __str__(&self) -> String {
            self.__repr__()
        }
    }

    #[pymethods]
    impl Wrapper {
        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }
    }

    #[pyfunction(name = "load_decisions")]
    #[pyo3(text_signature = "(paths: List[String], compact: bool = False, /)")]
    fn load_decisions_py(paths: Vec<&str>, compact: bool) -> PyResult<Vec<Decision>> {
        let paths = paths.into_iter().map(|p| Path::new(p)).collect();
        Ok(load_decisions(paths, compact))
    }

    #[pyfunction(name = "load_decisions_parallel")]
    #[pyo3(text_signature = "(paths: List[String], threads: int, compact: bool = False, /)")]
    fn load_decisions_parallel_py(paths: Vec<&str>, threads: usize, compact: bool) -> PyResult<Vec<Decision>> {
        let paths = paths.into_iter().map(|p| Path::new(p)).collect();
        Ok(load_decisions_parallel(paths, threads, compact))
    }

    #[pyfunction(name = "load_wrapper")]
    #[pyo3(text_signature = "(path: String, /)")]
    fn load_wrapper_py(path: &str) -> PyResult<Wrapper> {
        let path = Path::new(path);
        Ok(load_wrapper(path))
    }

    #[pymodule]
    fn skypie_proto_messages(_py: Python, m: &PyModule) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(load_decisions_py, m)?)?;
        m.add_function(wrap_pyfunction!(load_decisions_parallel_py, m)?)?;
        m.add_function(wrap_pyfunction!(load_wrapper_py, m)?)?;
        m.add_class::<Assignment>()?;
        m.add_class::<Scheme>()?;
        m.add_class::<Decision>()?;
        m.add_class::<OptimalByOptimizer>()?;
        m.add_class::<Run>()?;
        m.add_class::<TierAdvise>()?;
        m.add_class::<Setting>()?;
        m.add_class::<Wrapper>()?;
        Ok(())
    }
}

pub use messages::{
    load_decisions, load_decisions_parallel, load_wrapper, Assignment, Decision, OptimalByOptimizer, Run, Scheme, Setting,
    TierAdvise, Wrapper,
};
