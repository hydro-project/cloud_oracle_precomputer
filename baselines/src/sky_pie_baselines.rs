use pyo3::prelude::*;

mod optimizer;
use optimizer::Optimizer;
use optimizer::OptimizerData;

mod profit;
use profit::ProfitBasedOptimizer;
mod kmeans;
use kmeans::KmeansOptimizer;
mod workload;
use workload::Workload;

#[pymodule]
fn sky_pie_baselines(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ProfitBasedOptimizer>()?;
    m.add_class::<KmeansOptimizer>()?;
    m.add_class::<Workload>()?;
    Ok(())
}