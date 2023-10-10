use pyo3::prelude::*;

pub mod profit;
pub use profit::ProfitBasedOptimizer;
pub mod workload;
pub use workload::Workload;

#[pymodule]
fn sky_pie_baselines(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ProfitBasedOptimizer>()?;
    m.add_class::<Workload>()?;
    Ok(())
}