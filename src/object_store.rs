use serde::Deserialize;

use crate::network_record::NetworkCostMap;
use crate::{region::Region, range::Range};
use std::f64::{NEG_INFINITY,INFINITY};
use std::hash::{Hash, Hasher};

#[derive(Clone,Debug)]
pub struct Cost {
    pub size_cost: f64,
    pub put_cost: f64,
    pub put_transfer: f64,
    pub get_cost: f64,
    pub get_transfer: f64,
    pub egress_cost: NetworkCostMap,
    pub ingress_cost: NetworkCostMap
}

impl Cost {
    pub fn new(price_per_unit: f64, group: &str) -> Self {
        let mut cost = Self {
            size_cost: 0.0,
            put_cost: 0.0,
            put_transfer: 0.0,
            get_cost: 0.0,
            get_transfer: 0.0,
            egress_cost: NetworkCostMap::new(),
            ingress_cost: NetworkCostMap::new()
        };
        
        // Set cost according to group
        match group {
            "get request" => cost.get_cost = price_per_unit,
            "get transfer" => cost.get_transfer = price_per_unit,
            "put request" => cost.put_cost = price_per_unit,
            "put transfer" => cost.put_transfer = price_per_unit,
            "storage" => cost.size_cost = price_per_unit,
            _ => println!("Warning unknown price group: {}", group)
        }

        return cost;
    }

    pub fn get_egress_cost(&self, region: &Region) -> f64 {
        *self.egress_cost.get(&region).unwrap()
    }

    fn maxNetworkCosts(o: NetworkCostMap, p: NetworkCostMap, o_transfer_cost: f64, p_transfer_cost: f64) -> NetworkCostMap {
        if o.len() == 0 {
            p
        }
        else if p.len() == 0 {
            o
        }
        else if o_transfer_cost >= p_transfer_cost {
            o
        }
        else {
            p
        }
    }

    // Merge with other costs by maximum
    pub fn merge(&mut self, other: Self) {
        self.get_cost = self.get_cost.max(other.get_cost);
        self.get_transfer = self.get_transfer.max(other.get_transfer);
        self.put_cost = self.put_cost.max(other.put_cost);
        self.put_transfer = self.put_transfer.max(other.put_transfer);
        self.size_cost = self.size_cost.max(other.size_cost);
        //self.egress_cost = Self::maxNetworkCosts(self.egress_cost, other.egress_cost, self.get_transfer, other.get_transfer);
        //self.ingress_cost = Self::maxNetworkCosts(self.ingress_cost, other.ingress_cost, self.put_transfer, other.put_transfer);
    }

    pub fn add_ingress_costs(&mut self, mut ingress_cost: NetworkCostMap) {
        // Add put transfer costs to ingress costs
        for cost in ingress_cost.values_mut() {
            *cost = (*cost) + self.put_transfer;
        }
        
        self.ingress_cost = ingress_cost;
        self.egress_cost = NetworkCostMap::new();
    }

    pub fn add_egress_costs(&mut self, mut egress_cost: NetworkCostMap) {
        // Add get transfer costs to egress costs
        for cost in egress_cost.values_mut() {
            *cost = (*cost) + self.get_transfer;
        }
        self.egress_cost = egress_cost;
    }
}

#[derive(Clone,Debug,Deserialize)]
pub struct ObjectStoreStructRaw
{
    //"Vendor", "Region", "Name", "Tier", "Group", /* "StartingRange", "EndingRange", */ "PricePerUnit"
    // Set column names for Deserializer
    #[serde(rename = "Vendor")]
    pub vendor: String,
    #[serde(rename = "Region")]
    pub region: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Tier")]
    pub tier: String,
    #[serde(rename = "Group")]
    pub group: String,
    #[serde(rename = "PricePerUnit")]
    pub price_per_unit: f64,
}

impl From<ObjectStoreStructRaw> for ObjectStoreStruct {
    fn from(raw: ObjectStoreStructRaw) -> Self {
        let region = Region{name: format!("{}-{}", raw.vendor, raw.region)};
        let name = format!("{}-{}", raw.name, raw.tier);
        let cost: Cost = Cost::new(raw.price_per_unit, &raw.group);
        Self {
            id: 0,
            region,
            name,
            cost
        }
    }
}

#[derive(Clone,Debug)]
pub struct ObjectStoreStruct {
    pub id: u16,
    pub region: Region,
    pub name: String,
    pub cost: Cost
    //pub get_cost: f64,
    //pub egress_cost: f64
}

impl PartialEq for ObjectStoreStruct {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ObjectStoreStruct {}

impl Hash for ObjectStoreStruct {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

pub type ObjectStore = Box<ObjectStoreStruct>;
type OptRange = (ObjectStore, Range);

// Implement slo compatibility check
impl ObjectStoreStruct {
    pub fn new(id: u16) -> ObjectStore {
        ObjectStore::new(ObjectStoreStruct {
            id: id,
            name: String::from(""),
            region: Region { name: String::from("") },
            cost: Cost { size_cost: 0.0, put_cost: 0.0, put_transfer: 0.0, get_cost: 0.0, get_transfer: 0.0, egress_cost: NetworkCostMap::new(), ingress_cost: NetworkCostMap::new() }
        })
    }

    pub fn is_compatible_with(&self, _r: &Region) -> bool {
        // ...
        true
    }
    // Cost of an object store for given workload
    /* pub fn cost(&self, get: &f64, egress: &Vec<f64>) -> f64 {
        self.get_cost * get + self.egress_cost * egress
    } */
    pub fn cost_probe(&self, size: &f64, region: &Region) -> f64 {
        let egress_costs = self.cost.get_egress_cost(region);
        self.cost.get_cost + egress_costs * size
    }
    // Implement cost delta between two object stores
    pub fn cost_delta(&self, other: &ObjectStoreStruct, region: &Region) -> f64 {
        let self_egress_cost = self.cost.get_egress_cost(region);
        let other_egress_cost = other.cost.get_egress_cost(region);
        (self.cost.get_cost - other.cost.get_cost) / (other_egress_cost - self_egress_cost)
    }

    // Implementation of intersection of object stores for a region
    // Return type is a tuple of tuples with (ObjectStore, float, float)
    // where the floats are the min and max of the intersection
    // of the object store with the region
    // Define type as generic
    
    pub fn intersect(o: ObjectStore, p: ObjectStore, r: &Region) -> [OptRange; 2] {

        // XXX: prevent hashmap lookup and use get instead
        let o_egress_cost = o.cost.get_egress_cost(r);
        let p_egress_cost = p.cost.get_egress_cost(r);

        // Prevent division by zero
        if o_egress_cost == p_egress_cost {
            if o.cost.get_cost <= p.cost.get_cost {
                return [(o, Range{min:NEG_INFINITY, max:INFINITY}), (p, Range{min:INFINITY, max:NEG_INFINITY})]
            }
            else {
                return [(p, Range{min:NEG_INFINITY, max:INFINITY}), (o, Range{min:INFINITY, max:NEG_INFINITY})]
            }
        }

        // Get intersection by cost delta
        let size = o.cost_delta(&p, r);
        // Probe which object store falls to either side of the intersection
        // and return the tuple of tuples
        let size_probe = size - 1.0;
        let cost_probe_o = o.cost_probe(&size_probe, r);
        let cost_probe_p = p.cost_probe(&size_probe, r);

        if cost_probe_o < cost_probe_p {
            return [(o, Range{min:NEG_INFINITY, max:size}), (p, Range{min:size, max:INFINITY})]
        }
        else {
            return [(p, Range{min:NEG_INFINITY, max:size}), (o, Range{min:size, max:INFINITY})]
        }
    }
}
