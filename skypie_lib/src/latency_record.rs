use std::collections::HashMap;

use serde::Deserialize;

use crate::region::Region;

#[derive(Debug, Deserialize)]
pub(crate) struct LatencyRecordRaw {
    src_vendor: String,
    src_region: String,
    dest_vendor: String,
    dest_region: String,
    latency: f64
}

#[derive(Debug,Clone)]
pub(crate) struct LatencyRecord {
    // As concatenated string of src_vendor and src_region
    pub src: Region,
    // As concatenated string of dest_vendor and dest_region
    pub dest: Region,
    pub latency: f64,
}

impl From<LatencyRecordRaw> for LatencyRecord {
    fn from(raw: LatencyRecordRaw) -> Self {
        let src_name = format!("{}-{}", raw.src_vendor, raw.src_region);
        let dest_name = format!("{}-{}", raw.dest_vendor, raw.dest_region);

        Self {
            src: Region{id: u16::MAX, name:src_name},
            dest: Region{id: u16::MAX, name:dest_name},
            latency: raw.latency,
        }
    }
}

pub type LatencyMap = HashMap<Region, f64>;
pub type LatencyMaps = HashMap<Region, LatencyMap>;