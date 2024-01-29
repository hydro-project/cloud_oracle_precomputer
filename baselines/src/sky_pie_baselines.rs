use pyo3::prelude::*;

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

#[pymodule]
fn sky_pie_baselines(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ProfitBasedOptimizer>()?;
    m.add_class::<KmeansOptimizer>()?;
    m.add_class::<Workload>()?;
    m.add_class::<PyLoader>()?;
    Ok(())
}