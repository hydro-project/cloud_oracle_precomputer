use serde::{Deserialize, Serialize};

use crate::{ApplicationRegion, Tombstone};
use crate::network_record::NetworkCostMap;
use crate::{region::Region, range::Range};
use std::collections::HashMap;
use std::f64::{NEG_INFINITY,INFINITY};
use std::hash::{Hash, Hasher};

use super::identifier::Identifier;

#[derive(Clone,Debug, Deserialize, Serialize)]
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

    /*
    Return the egress costs between an object store an the application region
    XXX: Ignoring application region's ingress costs, since always 0
    */
    pub fn get_egress_cost(&self, region: &ApplicationRegion, _object_store_region: &Region) -> f64 {

        *self.egress_cost.get(&region.region).expect(&format!("Egress cost not found for region: {:?}", &region.region))
    }

    /*
    Return the ingress costs between an object store an the application region
    */
    pub fn get_ingress_cost(&self, region: &ApplicationRegion, object_store_region: &Region) -> f64 {
        *self.ingress_cost.get(&region.region).unwrap() + region.get_egress_cost(object_store_region)
    }

    pub fn get_transfer_cost(&self, src_region: &Region, dst_object_store: &ObjectStore) -> f64 {

        // Cost for network egress at source object store for region of destination object store
        let egress_cost = self.egress_cost.get(&dst_object_store.region).unwrap();
        // Cost for network ingress at destination object store for region of source object store
        let ingress_cost = dst_object_store.cost.ingress_cost.get(src_region).unwrap();

        return egress_cost + ingress_cost;
    }

    pub fn get_migration_cost(&self, src_region: &Region, dst_object_store: &ObjectStore, object_num: u64, object_size: f64) -> f64 {

        // Cost for transferring the objects from the source object store to the destination object store
        let transfer_cost = self.get_transfer_cost(src_region, dst_object_store);
        // Cost for putting the objects into the destination object store
        let put_cost = dst_object_store.cost.put_cost;
        // Cost for getting the objects from the source object store
        let get_cost = self.get_cost;

        return transfer_cost * object_size * object_num as f64 + (put_cost + get_cost) * object_num as f64;
    }

    pub fn compute_read_costs(&self, region: &ApplicationRegion, object_store_region: &Region, gets: f64, egress: f64) -> f64 {
        let egress_costs = self.get_egress_cost(region, object_store_region);
        self.get_cost * gets + egress_costs * egress
    }

    #[allow(dead_code)]
    fn max_network_costs(o: NetworkCostMap, p: NetworkCostMap, o_transfer_cost: f64, p_transfer_cost: f64) -> NetworkCostMap {
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
        //self.egress_cost = NetworkCostMap::new();
    }

    pub fn add_egress_costs(&mut self, mut egress_cost: NetworkCostMap) {
        // Add get transfer costs to egress costs
        for cost in egress_cost.values_mut() {
            *cost = (*cost) + self.get_transfer;
        }
        self.egress_cost = egress_cost;
    }
}

impl Default for Cost {
    fn default() -> Self {
        Cost { size_cost: 0.0, put_cost: 0.0, put_transfer: 0.0, get_cost: 0.0, get_transfer: 0.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }
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
        let region = Region{id: u16::MAX, name: format!("{}-{}", raw.vendor, raw.region)};
        let name = format!("{}-{}", raw.name, raw.tier);
        let cost: Cost = Cost::new(raw.price_per_unit, &raw.group);
        Self {
            id: u16::MAX,
            region,
            name,
            cost
        }
    }
}

#[derive(Clone,Debug, serde::Serialize, serde::Deserialize)]
pub struct ObjectStoreStruct {
    pub id: u16,
    pub region: Region,
    pub name: String,
    pub cost: Cost
    //pub get_cost: f64,
    //pub egress_cost: f64
}

impl Tombstone for ObjectStoreStruct {
    fn tombstone() -> Self {
        ObjectStoreStruct {
            id: u16::MAX-1,
            name: "".to_string(),
            region: Region::default(),
            cost: Cost::default()
        }
    }

    fn is_tombstone(&self) -> bool {
        self.id == u16::MAX-1
    }
}

impl Identifier<u16> for ObjectStoreStruct {
    fn get_id(&self) -> u16 {
        self.id
    }
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
//pub type ObjectStore = ObjectStoreStruct;
type OptRange = (ObjectStore, Range);

// Implement slo compatibility check
impl ObjectStoreStruct {
    pub fn new(id: u16) -> ObjectStore {
        ObjectStore::new(ObjectStoreStruct {
            id: id,
            name: String::from(""),
            region: Region::default(),
            cost: Cost { size_cost: 0.0, put_cost: 0.0, put_transfer: 0.0, get_cost: 0.0, get_transfer: 0.0, egress_cost: NetworkCostMap::new(), ingress_cost: NetworkCostMap::new() }
        })
    }

    // Cost of an object store for given workload
    /* pub fn cost(&self, get: &f64, egress: &Vec<f64>) -> f64 {
        self.get_cost * get + self.egress_cost * egress
    } */
    pub fn cost_probe(&self, size: &f64, region: &ApplicationRegion) -> f64 {
        let egress_costs = self.get_egress_cost(&region);
        self.cost.get_cost + egress_costs * size
    }
    // Implement cost delta between two object stores
    pub fn cost_delta(&self, other: &ObjectStoreStruct, region: &ApplicationRegion) -> f64 {
        let self_egress_cost = self.get_egress_cost(region);
        let other_egress_cost = other.get_egress_cost(region);
        (self.cost.get_cost - other.cost.get_cost) / -(self_egress_cost - other_egress_cost)
    }

    // Implementation of intersection of object stores for a region
    // Return type is a tuple of tuples with (ObjectStore, float, float)
    // where the floats are the min and max of the intersection
    // of the object store with the region
    // Define type as generic
    
    pub fn intersect(o: ObjectStore, p: ObjectStore, r: &ApplicationRegion) -> [OptRange; 2] {

        // Note: Egress = Egress of object store + ingress of application region
        // XXX: prevent hashmap lookup and use get instead
        let o_egress_cost = o.get_egress_cost(&r);
        let p_egress_cost = p.get_egress_cost(&r);

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
        let cost_probe_o = o.cost.get_cost + o_egress_cost * size_probe;
        let cost_probe_p = p.cost.get_cost + p_egress_cost * size_probe;

        if cost_probe_o < cost_probe_p {
            return [(o, Range{min:NEG_INFINITY, max:size}), (p, Range{min:size, max:INFINITY})]
        }
        else {
            return [(p, Range{min:NEG_INFINITY, max:size}), (o, Range{min:size, max:INFINITY})]
        }

    }

    ///    Returns the egress cost from an object store to the application region.
    ///
    ///    # Arguments
    ///    - `region`: A reference to the `ApplicationRegion` representing the application region,
    ///      which is the destination of the egress traffic.
    ///
    ///    # Returns
    ///    - The egress cost as a `f64` value.
    pub fn get_egress_cost(&self, dst_region: &ApplicationRegion) -> f64 {
        self.cost.get_egress_cost(dst_region, &self.region)
    }

    ///    Returns the ingress cost into an object store from the application region.
    ///
    ///    # Arguments
    ///    - `region`: A reference to the `ApplicationRegion` representing the application region,
    ///      which is the source of the ingress traffic.
    ///
    ///    # Returns
    ///    - The ingress cost as a `f64` value.
    pub fn get_ingress_cost(&self, src_region: &ApplicationRegion) -> f64 {
        self.cost.get_ingress_cost(src_region, &self.region)
    }

    /// Returns the transfer cost between this object store and the destination object store.
    /// 
    /// The transfer cost includes both the network cost and the retrieval cost on both sides.
    ///
    /// # Arguments
    ///
    /// * `dst_object_store` - The destination object store.
    ///
    /// # Returns
    ///
    /// The transfer cost as a `f64` value.
    pub fn get_transfer_cost(&self, dst_object_store: &ObjectStore) -> f64 {
        self.cost.get_transfer_cost(&self.region, dst_object_store)
    }

    /// Calculates the cost of migrating objects from one object store to another.
    ///
    /// This method calculates the cost of migrating a specified number of objects from the source object store to the destination object store.
    ///
    /// # Arguments
    ///
    /// * `dst_object_store`: The destination object store.
    /// * `object_num`: The number of objects to migrate.
    /// * `object_size`: The size of each object to migrate.
    ///
    /// # Returns
    ///
    /// The cost of migration as a floating-point number.
    pub fn get_migration_cost(&self, dst_object_store: &ObjectStore, object_num: u64, object_size: f64) -> f64 {
        self.cost.get_migration_cost(&self.region, dst_object_store, object_num, object_size)
    }

    pub fn compute_read_costs(&self, region: &ApplicationRegion, gets: f64, egress: f64) -> f64 {
        self.cost.compute_read_costs(region, &self.region, gets, egress)
    }

    /// Returns the fully qualified name of the object store.
    /// 
    /// The fully qualified name of an object store is the name of the region concatenated with the name of the object store, separated by a hyphen.
    pub fn fully_qualified_name(&self) -> String {
        format!("{}-{}", self.region.name, self.name)
    }
}

impl Default for ObjectStore {
    fn default() -> Self {
        ObjectStore::new(ObjectStoreStruct{id: u16::MAX, name: "".to_string(), cost: Cost::default(), region: Region::default()})
    }
}

impl Tombstone for ObjectStore {
    fn tombstone() -> Self {
        ObjectStore::new(ObjectStoreStruct::tombstone())
    }

    fn is_tombstone(&self) -> bool {
        self.as_ref().is_tombstone()
    }
}
