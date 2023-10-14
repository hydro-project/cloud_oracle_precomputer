use pyo3::prelude::*;

#[pyclass]
pub struct Workload {
    pub size: f64,
    pub puts: f64,
    gets: Vec<f64>,
    ingress: Vec<f64>,
    egress: Vec<f64>,
}

#[pymethods]
impl Workload {
    #[new]
    pub fn new(
        size: f64,
        puts: f64,
        gets: Vec<f64>,
        ingress: Vec<f64>,
        egress: Vec<f64>,
    ) -> Workload {
        Workload {
            size,
            puts,
            gets,
            ingress,
            egress,
        }
    }

    pub fn get_gets(&self, application_region_id: usize) -> f64 {
        self.gets[application_region_id]
    }

    pub fn get_egress(&self, application_region_id: usize) -> f64 {
        self.egress[application_region_id]
    }

    pub fn get_ingress(&self, application_region_id: usize) -> f64 {
        self.ingress[application_region_id]
    }
}