use std::collections::HashMap;

use crate::{Region, object_store::ObjectStore, ApplicationRegion, compatibility_checker::CompatibilityChecker};

pub struct CompatibilityCheckerNetworkSLOs {
    network_latency: HashMap<Region, HashMap<Region, f64>>,
    network_slo: f64
}

impl CompatibilityCheckerNetworkSLOs {
    pub(crate) fn new(network_latency: HashMap<Region, HashMap<Region, f64>>, network_slo: f64) -> Self {
        Self {
            network_latency,
            network_slo
        }
    }
}

impl CompatibilityChecker for CompatibilityCheckerNetworkSLOs {
    fn is_compatible(&self, object_store: &ObjectStore, app: &ApplicationRegion) -> bool {

        let latency = self.network_latency.get(&object_store.region).expect("Missing source region in lateny data!").get(&app.region).expect("Missing destination region in latency data");
        
        let compat = latency <= &self.network_slo;

        return compat;
    }
}