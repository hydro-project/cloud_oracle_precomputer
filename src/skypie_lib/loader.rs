use std::{collections::HashMap, path::PathBuf};

use crate::{skypie_lib::{
    network_record::{NetworkRecord, NetworkRecordRaw, NetworkCostMaps},
    object_store::{ObjectStoreStruct, ObjectStoreStructRaw, ObjectStore}, region::Region, identifier::Identifier,
}, ApplicationRegion};
use itertools::Itertools;
use regex::Regex;

pub struct Loader {
    pub object_stores: Vec<ObjectStore>,
    pub app_regions: Vec<ApplicationRegion>,
}

impl Loader {
    pub fn new(network_file_path: &PathBuf, object_store_file_path: &PathBuf, region_pattern: &str) -> Loader {

        let (network_egress, regions, region_names) = Loader::load_network(network_file_path, region_pattern);
        
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
        let object_stores = Loader::load_object_stores(object_store_file_path, region_pattern, &network_egress, &network_ingress, &region_names);

        
        // Load application regions
        let app_regions = regions.into_iter().map(|region|{
            
            let egress_cost = network_egress.get(&region).unwrap().clone();
            let ingress_cost = network_ingress.get(&region).unwrap().clone();
            
            ApplicationRegion{region, egress_cost, ingress_cost}
        }).collect_vec();
        
        println!("Number of object stores: {}, number of regions: {}", object_stores.len(), app_regions.len());

        Loader {
            object_stores,
            app_regions,
        }
    }

    fn load_network(
        network_file_path: &PathBuf,
        region_pattern: &str,
    ) -> (NetworkCostMaps, Vec<Region>, HashMap<String, Region>) {
        
        let re = Regex::new(region_pattern).unwrap();
        
        let rdr2 = csv::Reader::from_path(network_file_path).unwrap();
        let iter2: csv::DeserializeRecordsIntoIter<std::fs::File, NetworkRecordRaw> =
        rdr2.into_deserialize();
        
        // Collect all region names, including the destination regions
        let regions = iter2
        .map(|r: Result<NetworkRecordRaw, csv::Error>| -> NetworkRecord { r.unwrap().into() })
        //.inspect(|r| println!("Raw network: {:?}", r))
        .filter(|r| re.is_match(&r.src.name) && re.is_match(&r.dest.name))
        .map(|r| {
            [r.src.clone(), r.dest.clone()]
        }).flatten().unique().sorted().enumerate().map(|(i, r)| Region{id: i as u16, name: r.name}).collect_vec();

        let region_names: HashMap<String, Region> = HashMap::from_iter(regions.iter().map(|r| (r.name.clone(), r.clone())));

        let rdr = csv::Reader::from_path(network_file_path).unwrap();
        let iter: csv::DeserializeRecordsIntoIter<std::fs::File, NetworkRecordRaw> =
            rdr.into_deserialize();

        let mut network_costs: NetworkCostMaps = iter
            .map(|r: Result<NetworkRecordRaw, csv::Error>| -> NetworkRecord { r.unwrap().into() })
            //.inspect(|r| println!("Raw network: {:?}", r))
            .filter(|r| re.is_match(&r.src.name) && re.is_match(&r.dest.name))
            //.inspect(|r| println!("Filtered network: {:?}", r))
            .fold(NetworkCostMaps::new(), |mut agg, e| {
                // Collect src/dest region in regions
                
                let src_region = region_names.get(&e.src.name).unwrap();

                let dest_region = region_names.get(&e.dest.name).unwrap();

                let src_map = agg.entry(src_region.clone()).or_insert(HashMap::new());
                src_map.insert(dest_region.clone(), e.cost);
                agg
            });

        // Collect all region names, including the destination regions
        /* let regions =
            /* network_costs
            .iter()
            .flat_map(|x| {
                let (src, dests) = x;
                let mut dests = dests.keys().map(|x| x.clone()).collect_vec();
                dests.push(src.clone());
                dests
            }) */
            region_names.values()
            .unique()
            // Set region IDs by sorting and enumerating them
            .sorted()
            .map(|r| r.to_owned())
            .collect_vec(); */

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

        // Validate application regions
        let min = regions.iter().map(|r|r.get_id()).min().unwrap();
        let max = regions.iter().map(|r|r.get_id()).max().unwrap();
        let unique = regions.iter().map(|r|r.get_id()).unique().collect_vec().len();

        if regions.len() != unique {
            for r in &regions {
                println!("Region: {:?}", r);
            }
        }
        assert_eq!(regions.len(), unique);
        assert_eq!(min, 0);
        assert_eq!(max as usize, regions.len() - 1);

        regions.iter().for_each(|r|{assert!(r.get_id() != u16::MAX, "Found region with id u16::MAX {:?}", r)});

        return (network_costs, regions, region_names);
    }

    fn load_object_stores(object_store_file_path: &PathBuf, region_pattern: &str, egress_costs: &NetworkCostMaps, ingress_costs: &NetworkCostMaps, region_names: &HashMap<String, Region>) -> Vec<ObjectStore> {
        let re = Regex::new(region_pattern).unwrap();

        let rdr = csv::Reader::from_path(object_store_file_path).unwrap();
        let iter: csv::DeserializeRecordsIntoIter<std::fs::File, ObjectStoreStructRaw> =
            rdr.into_deserialize();

        let object_stores: Vec<ObjectStore> = iter
            .map(|x| x.unwrap().into())
            .filter(|r: &ObjectStoreStruct| re.is_match(&r.region.name))
            
            // Combine object stores with identical names
            .fold(HashMap::new(), |mut agg, mut object_store| {
                let name = object_store.name.clone();
                let region_name = object_store.region.name.clone();
                let key = format!("{}-{}", name, region_name);

                // Set region with correct id
                if !region_names.contains_key(&region_name) {
                    println!("WARN: Region {:?} not found, skipping object store: {}", region_name, key);
                }
                else {
                    let region = region_names.get(&region_name).unwrap().clone();
                    object_store.region = region;
                    
                    let entry = agg.entry(key).or_insert(object_store.clone());
                    entry.cost.merge(object_store.cost);
                }

                agg
            }).into_iter()
            // Sort by name
            .sorted_by(|a,b|{a.0.cmp(&b.0)})
            .map(|(_n,  o)|{o})
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
            //.map(|x| ObjectStore::new(x))
            .collect_vec();

        let min = object_stores.iter().map(|r|r.get_id()).min().unwrap();
        let max = object_stores.iter().map(|r|r.get_id()).max().unwrap();
        let unique = object_stores.iter().map(|r|r.get_id()).unique().collect_vec().len();

        if object_stores.len() != unique {
            for r in &object_stores {
                println!("Object store: {:?}", r);
            }
        }
        assert_eq!(object_stores.len(), unique);
        assert_eq!(min, 0);
        assert_eq!(max as usize, object_stores.len() - 1);

        return object_stores;
    }
}
