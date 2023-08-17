use std::hash::{Hash, Hasher};
use crate::Region;
use super::{network_record::NetworkCostMap, identifier::Identifier};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct ApplicationRegion {
    pub region: Region,
    pub egress_cost: NetworkCostMap,
    pub ingress_cost: NetworkCostMap
}

impl ApplicationRegion {
    pub fn get_egress_cost(&self, region: &Region) -> f64 {
        *self.egress_cost.get(&region).unwrap()
    }

    pub fn get_ingress_cost(&self, region: &Region) -> f64 {
        *self.ingress_cost.get(&region).unwrap()
    }
}

impl Identifier<u16> for ApplicationRegion {
    fn get_id(self: &Self) -> u16 {
        self.region.get_id()
    }
}

impl PartialEq for ApplicationRegion {
    fn eq(&self, other: &Self) -> bool {
        self.get_id() == other.get_id()
    }
}

impl Eq for ApplicationRegion {}

impl Hash for ApplicationRegion {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_id().hash(state);
    }
}

// Create a static instance of the default region
lazy_static! {
    static ref DEFAULT_APPLICATION_REGION: ApplicationRegion = {
        ApplicationRegion {
            region: Region{id: u16::MAX, name: String::from("empty")},
            egress_cost: NetworkCostMap::default(),
            ingress_cost: NetworkCostMap::default()
        }
    };
}

impl Default for &ApplicationRegion {
    fn default() -> Self {
        &DEFAULT_APPLICATION_REGION
    }
}

impl Identifier<u16> for &ApplicationRegion {
    fn get_id(self: &Self) -> u16 {
        (*self).get_id()
    }
}