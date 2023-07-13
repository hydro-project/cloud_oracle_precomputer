use std::time::SystemTime;

use chrono::{DateTime, Utc};
use influxdb::InfluxDbWriteable;

#[derive(InfluxDbWriteable, Debug)]
pub struct SkyPieLogEntry {
    time: DateTime<Utc>,
    cycle_time: f64,
    count: u64,
    #[influxdb(tag)] epoch: u64,
    #[influxdb(tag)] stage: String,
}

impl SkyPieLogEntry {
    pub fn new(cycle_time: f64, count: u64, epoch: u64, stage: String) -> Self {
        Self {
            time: SystemTime::now().into(),
            cycle_time,
            count,
            epoch,
            stage,
        }
    }
}