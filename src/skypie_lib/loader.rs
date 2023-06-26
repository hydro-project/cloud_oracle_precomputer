use std::{collections::HashMap, path::PathBuf};

use crate::skypie_lib::{
    network_record::{NetworkRecord, NetworkRecordRaw, NetworkCostMaps},
    object_store::{ObjectStoreStruct, ObjectStoreStructRaw, ObjectStore}, region::Region,
};
use itertools::Itertools;
use regex::Regex;

pub struct Loader {
    pub object_stores: Vec<ObjectStore>,
    pub regions: Vec<Region>,
}

impl Loader {
    pub fn new(network_file_path: &PathBuf, object_store_file_path: &PathBuf, region_pattern: &str) -> Loader {

        let (network_egress, regions) = Loader::load_network(network_file_path, region_pattern);
        
        // Load network ingress costs, currently 0.0
        let network_ingress = network_egress
            .iter()
            .fold(NetworkCostMaps::new(), |mut agg, e| {
                
                let (src, dests) = e;
                
                for (dest, _cost) in dests.iter() {
                    let dest_map = agg.entry(dest.clone()).or_insert(HashMap::new());
                    dest_map.insert(
                        src.clone(),
                        0.0
                    );
                }
                agg
            });

        assert_eq!(network_egress.len(), network_ingress.len());
        assert!(!network_egress.is_empty());
        assert!(!network_ingress.is_empty());
        assert!(!regions.is_empty());

        // Load object stores
        let object_stores = Loader::load_object_stores(object_store_file_path, region_pattern, &network_egress, &network_ingress);

        Loader {
            object_stores,
            regions,
        }
    }

    fn load_network(
        network_file_path: &PathBuf,
        region_pattern: &str,
    ) -> (NetworkCostMaps, Vec<Region>) {
        let re = Regex::new(region_pattern).unwrap();

        let rdr = csv::Reader::from_path(network_file_path).unwrap();
        let iter: csv::DeserializeRecordsIntoIter<std::fs::File, NetworkRecordRaw> =
            rdr.into_deserialize();
        let mut network_costs: NetworkCostMaps = iter
            .map(|r: Result<NetworkRecordRaw, csv::Error>| -> NetworkRecord { r.unwrap().into() })
            //.inspect(|r| println!("Raw network: {:?}", r))
            .filter(|r| re.is_match(&r.src.name) || re.is_match(&r.dest.name))
            //.inspect(|r| println!("Filtered network: {:?}", r))
            .fold(NetworkCostMaps::new(), |mut agg, e| {
                let src_map = agg.entry(e.src.clone()).or_insert(HashMap::new());
                src_map.insert(e.dest.clone(), e.cost);
                agg
            });

        // Collect all region names, including the destination regions
        let regions = network_costs
            .iter()
            .flat_map(|x| {
                let (src, dests) = x;
                let mut dests = dests.keys().map(|x| x.clone()).collect_vec();
                dests.push(src.clone());
                dests
            })
            .unique()
            .collect_vec();

        // Ensure each region has network costs to itself
        for region in regions.iter() {
            let region_map = network_costs
                .entry(region.clone())
                .or_insert(HashMap::new());
            region_map.insert(
                region.clone(),
                0.0,
            );
        }

        return (network_costs, regions);
    }

    fn load_object_stores(object_store_file_path: &PathBuf, region_pattern: &str, egress_costs: &NetworkCostMaps, ingress_costs: &NetworkCostMaps) -> Vec<ObjectStore> {
        let re = Regex::new(region_pattern).unwrap();

        let rdr = csv::Reader::from_path(object_store_file_path).unwrap();
        let iter: csv::DeserializeRecordsIntoIter<std::fs::File, ObjectStoreStructRaw> =
            rdr.into_deserialize();

        let object_stores: Vec<ObjectStore> = iter
            .map(|x| x.unwrap().into())
            .filter(|r: &ObjectStoreStruct| re.is_match(&r.region.name))
            
            // Combine object stores with identical names
            .fold(HashMap::new(), |mut agg, object_store| {
                let name = object_store.name.clone();
                let region = object_store.region.name.clone();
                let key = format!("{}-{}", name, region);
                let entry = agg.entry(key).or_insert(object_store.clone());
                entry.cost.merge(object_store.cost);

                agg
            }).values()
            // Filter object stores whose region does not have network costs
            .inspect(|x|{
                if egress_costs.contains_key(&x.region) && ingress_costs.contains_key(&x.region) {
                    //println!("Found network costs for {:?}, in {:?}", x.name, x.region);
                } else {
                    println!("WARN: No network costs for {:?}, in {:?}", x.name, x.region);
                }
            })
            .filter(|x| egress_costs.contains_key(&x.region) && ingress_costs.contains_key(&x.region))
            // Set ids of object stores by enumerating them
            .enumerate()
            .map(|(i, x)| {
                let mut x = x.clone();
                x.id = i as u16;
                x
            })
            // Add network egress/ingress costs
            .map(|x| {
                let mut x = x.clone();
                x.cost.add_ingress_costs(ingress_costs.get(&x.region).unwrap().clone());
                x.cost.add_egress_costs(egress_costs.get(&x.region).unwrap().clone());
                x
            })
            .map(|x| ObjectStore::new(x))
            .collect_vec();

        return object_stores;
    }
}
