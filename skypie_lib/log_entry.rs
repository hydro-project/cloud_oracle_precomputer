use std::time::SystemTime;

use chrono::{DateTime, Utc};
use influxdb::InfluxDbWriteable;

#[derive(InfluxDbWriteable, Debug)]
pub struct SkyPieLogEntry {
    time: DateTime<Utc>,
    cycle_time: f64,
    count: u64,
    #[influxdb(tag)] stage: String,
    #[influxdb(tag)] experiment: String,
}

impl SkyPieLogEntry {
    pub fn new(cycle_time: f64, count: u64, stage: String, experiment: String) -> Self {
        Self {
            time: SystemTime::now().into(),
            cycle_time,
            count,
            stage,
            experiment,
        }
    }
}