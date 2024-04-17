use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;

mod optimizer;
use optimizer::Optimizer;

mod profit;
use profit::ProfitBasedOptimizer;
mod kmeans;
use kmeans::KmeansOptimizer;
mod workload;
use workload::Workload;
mod py_loader;
use py_loader::PyLoader;



#[pyfunction]
fn spanstore_aggregate(requests: &PyList, objects_in_access_set: Vec<&str>) -> (HashMap<String, i64>,HashMap<String, i64>,HashMap<String, f64>,HashMap<String, f64>) /* PyResult<PyTuple> */ {

    let mut put_counts = HashMap::<String, i64>::new();
    let mut get_counts = HashMap::<String, i64>::new();
    let mut ingress_counts = HashMap::<String, f64>::new();
    let mut egress_counts = HashMap::<String, f64>::new();

    // Convert object_in_access_set to a HashSet
    let objects_in_access_set: std::collections::HashSet<&str> = objects_in_access_set.iter().cloned().collect();

    // Iterate over the list of requests and print them
    for request in requests.iter() {
        // Get request type
        let obj_key = request.getattr("obj_key").unwrap().extract::<String>().unwrap();
        if objects_in_access_set.contains(&obj_key.as_str()) {
            let op = request.getattr("op").unwrap().extract::<String>().unwrap();
            let issue_region = request.getattr("issue_region").unwrap().extract::<String>().unwrap();
            let size = request.getattr("size").unwrap().extract::<f64>().unwrap();

            if op == "write" {
                let put_count = put_counts.entry(issue_region.clone()).or_insert(0);
                *put_count += 1;
                let ingress_count = ingress_counts.entry(issue_region.clone()).or_insert(0.0);
                *ingress_count += size;
            } else if op == "read" {
                let get_count = get_counts.entry(issue_region.clone()).or_insert(0);
                *get_count += 1;
                let egress_count = egress_counts.entry(issue_region.clone()).or_insert(0.0);
                *egress_count += size;
            }
        }

    }
    
    /* println!("put_counts: {:?} ", put_counts);
    println!("get_counts: {:?}", get_counts);
    println!("ingress_counts: {:?}", ingress_counts);
    println!("egress_counts: {:?}", egress_counts); */
    
    return (put_counts, get_counts, ingress_counts, egress_counts); //.into_py();
}

#[pymodule]
fn sky_pie_baselines(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ProfitBasedOptimizer>()?;
    m.add_class::<KmeansOptimizer>()?;
    m.add_class::<Workload>()?;
    m.add_class::<PyLoader>()?;
    m.add_wrapped(wrap_pyfunction!(spanstore_aggregate))?;
    Ok(())
}