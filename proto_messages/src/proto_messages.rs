pub mod protobuf_file_reader;
pub mod protobuf_file_sink;

pub use protobuf_file_reader::ProtobufFileReader;
pub use protobuf_file_sink::ProtobufFileSink;

#[allow(non_snake_case)]
mod messages {
    use crate::{ProtobufFileReader, ProtobufFileSink};
    use hydroflow::futures::SinkExt;
    use numpy::{PyArrayDyn, PyArray};
    use prost::Message;
    #[cfg(feature = "python-module")]
    use pyo3::prelude::*;
    use pyo3::{pyfunction, Python};
    use rayon::prelude::*;
    use std::{collections::HashMap, path::Path, fs};

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

        pub fn combine(&mut self, other: &Self, original_path: &Path, output_path: &Path, output_path_suffix: &Path, replace_with_candidates: bool) {
            println!("Combining wrapper");

            let tier_advise = self.tier_advise.as_mut().unwrap();
            let other_tier_advise = other.tier_advise.as_ref().unwrap();

            tier_advise.combine(other_tier_advise, original_path, output_path, output_path_suffix, replace_with_candidates);
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

        pub fn combine(&mut self, other: &Self, original_path: &Path, output_path: &Path, output_path_suffix: &Path, replace_with_candidates: bool) {
            println!("Combining tier advise");
            let self_first_entry = self.replication_factor.iter_mut().next().unwrap();
            for (_replication_factor, setting) in &other.replication_factor {
                self_first_entry.1.combine(setting, original_path, output_path, output_path_suffix, replace_with_candidates);
            }
        }
    }

    impl Setting {
        pub fn combine(&mut self, other: &Self, original_path: &Path, output_path: &Path, output_path_suffix: &Path, replace_with_candidates: bool) {
            println!("Combining settings");
            let my_first_run = self.runs.iter_mut().next().unwrap();
            for (_run_name, run) in &other.runs {
                
                    my_first_run.1.combine(run, original_path, output_path, output_path_suffix, replace_with_candidates);
            }
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

        pub fn combine(&mut self, other: &Self, original_path: &Path, output_path: &Path, output_path_suffix: &Path, replace_with_candidates: bool) {
            println!("Combining runs");

            self.enumerator_time_ns = Some(self.enumerator_time_ns.unwrap() + other.enumerator_time_ns.unwrap());
            self.min_replication_factor = Some(self.min_replication_factor.unwrap().min(other.min_replication_factor.unwrap()));
            self.max_replication_factor = Some(self.max_replication_factor.unwrap().max(other.max_replication_factor.unwrap()));
            assert_eq!(self.no_app_regions.unwrap(), other.no_app_regions.unwrap());
            assert_eq!(self.no_dimensions.unwrap(), other.no_dimensions.unwrap());
            self.no_facets = Some(self.no_facets.unwrap() + other.no_facets.unwrap());
            self.partitioner_time_ns = Some(self.partitioner_time_ns.unwrap() + other.partitioner_time_ns.unwrap());
            //self.no_object_stores = Some(self.no_object_stores.unwrap().max(other.no_object_stores.unwrap()));
            assert_eq!(self.object_stores_considered, other.object_stores_considered);

            let candidates = if replace_with_candidates {Some(&other.candidate_partitions)} else {None};

            let my_first_optimizer = self.optimal_partitions_by_optimizer.iter_mut().next().unwrap();

            for (_optimizer_name, optimal_by_optimizer) in &other.optimal_partitions_by_optimizer {
                my_first_optimizer.1.combine(optimal_by_optimizer, original_path, output_path, output_path_suffix, candidates);
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

        pub fn combine(&mut self, other: &Self, original_path: &Path, output_path: &Path, output_path_suffix: &Path, candidates: Option<&Vec<String>>) {
            println!("Combining optimal partitions");
            //assert_eq!(self.optimizer_type, other.optimizer_type);
            self.no_facets = Some(self.no_facets.unwrap() + other.no_facets.unwrap());
            self.partitioner_time_ns = Some(self.partitioner_time_ns.unwrap() + other.partitioner_time_ns.unwrap());
            self.partitioner_computation_time_ns = Some(self.partitioner_computation_time_ns.unwrap() + other.partitioner_computation_time_ns.unwrap());

            let partitions = if let Some(candidates) = candidates {
                candidates
            } else {
                &other.optimal_partitions
            };

            for partition in partitions {
                let output_suffix = output_path_suffix.join(partition);
                self.optimal_partitions.push(output_suffix.to_str().unwrap().to_string());
            }

            partitions.par_iter().for_each(|partition|{
                let output_suffix = output_path_suffix.join(partition);

                // Copy file from original location to output location
                let original_file = original_path.join(partition);
                let output_file = output_path.join(output_suffix);
                let copy_error_msg = format!("Failed to copy file {} to {}", original_file.to_string_lossy(), output_file.to_string_lossy());
                println!("Copying file {} to {}", original_file.to_string_lossy(), output_file.to_string_lossy());
                fs::copy(original_file, output_file).expect(copy_error_msg.as_str());
            });
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

    pub fn load_decision_costs(paths: Vec<&Path>) -> Vec<Vec<f32>> {
        let mut res = Vec::new();

        for path in paths {
            let message_iter = ProtobufFileReader::new(path).unwrap().into_iter_all::<Decision>();

            res.extend(message_iter.map(|x: Decision| {
                let len = x.cost_wl_halfplane.len();
                x.cost_wl_halfplane.into_iter().take(len - 1).map(|x| x as f32).collect::<Vec<f32>>()
            }));
        }

        return res;
    }

    pub fn load_decision_costs_parallel(paths: Vec<&Path>, threads: usize) -> Vec<Vec<f32>> {

        let chunk_size = std::cmp::max(1, paths.len() / threads);
        let res = paths.chunks(chunk_size).into_iter().par_bridge()
            .map(|chunk|load_decision_costs(chunk.to_vec()))
            .reduce(|| vec![], |mut acc: Vec<Vec<f32>>, next| {acc.extend(next); acc});

        return res;
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

    pub fn count_decisions(paths: Vec<&Path>) -> usize {
        type M = Decision;

        let decisions = paths.into_iter().map(|path| {
            let message_iter = ProtobufFileReader::new(path).unwrap().into_iter_all::<M>();
            message_iter.count()
        }).sum();

        decisions
    }

    pub fn load_decisions_parallel(paths: Vec<&Path>, threads: usize, compact: bool) -> Vec<Decision> {

        let chunk_size = std::cmp::max(1, paths.len() / threads);
        paths.chunks(chunk_size).into_iter().par_bridge()
            .map(|chunk|load_decisions(chunk.to_vec(), compact))
            .reduce(|| vec![], |mut acc: Vec<Decision>, next| {acc.extend(next); acc})
    }

    pub fn count_decisions_parallel(paths: Vec<&Path>, threads: usize) -> usize {

        let chunk_size = std::cmp::max(1, paths.len() / threads);
        paths.chunks(chunk_size).into_iter().par_bridge()
            .map(|chunk|count_decisions(chunk.to_vec()))
            .sum()
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
        load_decision_costs, load_decision_costs_parallel,
        load_decisions, load_decisions_parallel, load_wrapper,
        count_decisions, count_decisions_parallel,
        Assignment, Decision, OptimalByOptimizer, Run, Scheme,
        Setting, TierAdvise, Wrapper,
    };
    use pyo3::prelude::*;
    use pyo3::pymethods;
    use pyo3::wrap_pyfunction;
    use numpy::{PyArray, PyArrayDyn};
    use std::path::Path;
    use std::mem::size_of_val;
    use crate::ProtobufFileReader;
    use rayon::iter::{ParallelBridge,IntoParallelIterator,ParallelIterator};

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
        fn combine_py(&mut self, other: &Self, original_path: &str, output_path: &str, output_path_suffix: &str, replace_with_candidates: bool) {
            self.combine(other, Path::new(original_path), Path::new(output_path), Path::new(output_path_suffix), replace_with_candidates);
        }

        fn save_py(&self, path: &str) {
            self.save(path);
        }

        fn __repr__(&self) -> String {
            format!("{:?}", self)
        }

        fn __str__(&self) -> String {
            self.__repr__()
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

    #[pyfunction(name = "load_decision_costs")]
    #[pyo3(text_signature = "(paths: List[String], /)")]
    fn load_decision_costs_py(paths: Vec<&str>) -> PyResult<Vec<Vec<f32>>> {
        let paths = paths.into_iter().map(|p| Path::new(p)).collect();
        Ok(load_decision_costs(paths))
    }

    #[pyfunction(name = "load_decision_costs_parallel")]
    #[pyo3(text_signature = "(paths: List[String], threads: int, /)")]
    fn load_decision_costs_parallel_py(paths: Vec<&str>, threads: usize) -> PyResult<Vec<Vec<f32>>> {
        let paths = paths.into_iter().map(|p| Path::new(p)).collect();
        Ok(load_decision_costs_parallel(paths, threads))
    }

    #[pyfunction(name = "load_decision_costs_parallel_with_size")]
    #[pyo3(text_signature = "(paths: List[String], threads: int, /)")]
    fn load_decision_costs_parallel_with_size_py(paths: Vec<&str>, threads: usize) -> PyResult<(Vec<Vec<f32>>, usize)> {
        let paths = paths.into_iter().map(|p| Path::new(p)).collect();
        let res  = load_decision_costs_parallel(paths, threads);
        let size = res.len() * res[0].len() * size_of_val(&res[0][0]);
        Ok((res, size))
    }
    
    #[pyfunction(name = "count_decisions")]
    #[pyo3(text_signature = "(paths: List[String], /)")]
    fn count_decisions_py(paths: Vec<&str>) -> PyResult<usize> {
        let paths = paths.into_iter().map(|p| Path::new(p)).collect();
        Ok(count_decisions(paths))
    }

    #[pyfunction(name = "count_decisions_parallel")]
    #[pyo3(text_signature = "(paths: List[String], threads: int, /)")]
    fn count_decisions_parallel_py(paths: Vec<&str>, threads: usize) -> PyResult<usize> {
        let paths = paths.into_iter().map(|p| Path::new(p)).collect();
        Ok(count_decisions_parallel(paths, threads))
    }

    #[pyfunction(name = "load_wrapper")]
    #[pyo3(text_signature = "(path: String, /)")]
    fn load_wrapper_py(path: &str) -> PyResult<Wrapper> {
        let path = Path::new(path);
        Ok(load_wrapper(path))
    }

    #[pyfunction]
    pub fn load_decision_costs_numpy<'py>(py: Python<'py>, paths: Vec<String>) -> &'py PyArrayDyn<f32> {
        
            let path = Path::new(paths.first().unwrap());
            let cols = ProtobufFileReader::new(&path).unwrap().into_iter_all::<Decision>().take(1).map(|x: Decision| {
                x.cost_wl_halfplane.len()
            }).reduce(|a, b| a.max(b)).unwrap();
            let cols = cols - 1; // Skip cost dimension

            let iter = paths.into_iter()
            .flat_map(|path|{
                let path = Path::new(&path);
                let message_iter = ProtobufFileReader::new(path).unwrap().into_iter_all::<Decision>();
                let c = message_iter.flat_map(|x: Decision| {
                    x.cost_wl_halfplane.into_iter().take(cols).map(|x| x as f32)
                });
                c
            }); //.collect::<Vec<f32>>();
            let pyarray_1dim = PyArray::from_iter(py, iter);
            
            // Reshape to 2 dimensions
            // Check that the number of elements is multiple of cols
            if pyarray_1dim.len() % cols != 0 {
                panic!("Inconsistent number of elements in decision costs");
            };
            let rows = pyarray_1dim.len() / cols;
            let dims = vec![rows, cols];
            let pyarray_2dim: &PyArrayDyn<f32> = pyarray_1dim.reshape(dims).unwrap();

            pyarray_2dim
    }

    #[pyfunction]
    pub fn load_decision_costs_numpy_parallel<'py>(py: Python<'py>, paths: Vec<String>) -> &'py PyArrayDyn<f32> {
        
            let path = Path::new(paths.first().unwrap());
            let cols = ProtobufFileReader::new(&path).unwrap().into_iter_all::<Decision>().take(1).map(|x: Decision| {
                x.cost_wl_halfplane.len()
            }).reduce(|a, b| a.max(b)).unwrap();
            let cols = cols - 1; // Skip cost dimension

            let iter = paths.into_par_iter() //.into_iter()
            .flat_map(|path|{
                let path = Path::new(&path);
                let message_iter = ProtobufFileReader::new(path).unwrap().into_iter_all::<Decision>();
                let c = message_iter.flat_map(|x: Decision| {
                    x.cost_wl_halfplane.into_iter().take(cols).map(|x| x as f32)
                }).par_bridge();
                c
            }).collect::<Vec<f32>>();
            let pyarray_1dim = PyArray::from_iter(py, iter);
            
            // Reshape to 2 dimensions
            // Check that the number of elements is multiple of cols
            if pyarray_1dim.len() % cols != 0 {
                panic!("Inconsistent number of elements in decision costs");
            };
            let rows = pyarray_1dim.len() / cols;
            let dims = vec![rows, cols];
            let pyarray_2dim: &PyArrayDyn<f32> = pyarray_1dim.reshape(dims).unwrap();

            pyarray_2dim
    }

    #[pymodule]
    fn skypie_proto_messages(_py: Python, m: &PyModule) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(load_decisions_py, m)?)?;
        m.add_function(wrap_pyfunction!(load_decisions_parallel_py, m)?)?;
        m.add_function(wrap_pyfunction!(load_decision_costs_py, m)?)?;
        m.add_function(wrap_pyfunction!(load_decision_costs_numpy, m)?)?;
        m.add_function(wrap_pyfunction!(load_decision_costs_numpy_parallel, m)?)?;
        m.add_function(wrap_pyfunction!(load_decision_costs_parallel_py, m)?)?;
        m.add_function(wrap_pyfunction!(load_decision_costs_parallel_with_size_py, m)?)?;
        m.add_function(wrap_pyfunction!(count_decisions_py, m)?)?;
        m.add_function(wrap_pyfunction!(count_decisions_parallel_py, m)?)?;
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
    load_decision_costs, load_decision_costs_parallel, load_decisions, load_decisions_parallel, load_wrapper, count_decisions, count_decisions_parallel, Assignment, Decision, OptimalByOptimizer, Run, Scheme, Setting,
    TierAdvise, Wrapper,
};
