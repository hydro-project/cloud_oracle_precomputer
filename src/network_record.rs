use std::collections::HashMap;

use serde::Deserialize;

use crate::region::Region;

#[derive(Debug, Deserialize)]
pub(crate) struct NetworkRecordRaw {
    src_vendor: String,
    src_region: String,
    dest_vendor: String,
    dest_region: String,
    cost: f64
}

#[derive(Debug,Clone)]
pub(crate) struct NetworkRecord {
    // As concatenated string of src_vendor and src_region
    pub src: Region,
    // As concatenated string of dest_vendor and dest_region
    pub dest: Region,
    pub cost: f64,
}

impl From<NetworkRecordRaw> for NetworkRecord {
    fn from(raw: NetworkRecordRaw) -> Self {
        let src_name = format!("{}-{}", raw.src_vendor, raw.src_region);
        let dest_name = format!("{}-{}", raw.dest_vendor, raw.dest_region);

        Self {
            src: Region{name:src_name},
            dest: Region{name:dest_name},
            cost: raw.cost,
        }
    }
}

pub type NetworkCostMap = HashMap<Region, f64>;
pub type NetworkCostMaps = HashMap<Region, NetworkCostMap>;