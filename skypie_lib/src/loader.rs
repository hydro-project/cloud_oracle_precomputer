use std::{collections::{HashMap, HashSet}, path::PathBuf};

use crate::{
    network_record::{NetworkRecord, NetworkRecordRaw, NetworkCostMaps},
    object_store::{ObjectStoreStruct, ObjectStoreStructRaw, ObjectStore}, region::Region, identifier::Identifier,
    ApplicationRegion, compatibility_checker::{CompatibilityChecker, DefaultCompatibilityChecker}, latency_record::{LatencyRecordRaw, LatencyRecord, LatencyMaps, LatencyMap}, compatibility_checker_network_slos::CompatibilityCheckerNetworkSLOs
};
use itertools::Itertools;
use regex::Regex;

pub struct Loader {
    pub object_stores: Vec<ObjectStore>,
    pub app_regions: Vec<ApplicationRegion>,
    pub network_latency: LatencyMaps,
    pub compatibility_checker_slos: Box<dyn CompatibilityChecker>,
}

impl Loader {
    pub fn with_region_and_object_store_names(network_file_path: &PathBuf, object_store_file_path: &PathBuf, region_list: Vec<Region>, object_store_list: &Vec<String>, latency_file_path: &Option<PathBuf>, latency_slo: &Option<f64>, verbose: Option<i32>) -> Self {
        Loader::load(network_file_path, object_store_file_path, None, None, Some(region_list), Some(object_store_list), latency_file_path, latency_slo, verbose)
    }

    pub fn new(network_file_path: &PathBuf, object_store_file_path: &PathBuf, region_pattern: &str, object_store_pattern: &str, latency_file_path: &Option<PathBuf>, latency_slo: &Option<f64>, verbose: Option<i32>) -> Loader {
        Loader::load(network_file_path, object_store_file_path, Some(region_pattern), Some(object_store_pattern), None, None, latency_file_path, latency_slo, verbose)
    }

    fn load(network_file_path: &PathBuf, object_store_file_path: &PathBuf, region_pattern: Option<&str>, object_store_pattern: Option<&str>, region_list: Option<Vec<Region>>, object_store_list: Option<&Vec<String>>, latency_file_path: &Option<PathBuf>, latency_slo: &Option<f64>, verbose: Option<i32>) -> Loader {

        let verbose = verbose.unwrap_or(0);
        let (network_egress, regions, region_names) = Loader::load_network(network_file_path, region_pattern, region_list, Some(verbose));
        
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


        // Load network latency and compatibility checker
        let (network_latency, region_names, regions, network_egress, network_ingress) = if latency_file_path.is_some() && latency_slo.is_some() {
            let latency_file_path = latency_file_path.clone().unwrap();
            assert!(latency_file_path.exists());

            let (network_latency, region_names_with_latency_data) = Loader::load_latency(&latency_file_path, &region_names, Some(verbose));

            // Align network regions with regions that have latency data: filter out regions without latency data and update region IDs
            let network_egress = network_egress.into_iter().filter(|(region, _costs)|region_names_with_latency_data.contains_key(&region.name)).map(|(region, costs)|{
                let region = region_names_with_latency_data.get(&region.name).unwrap().clone();
                // Translate costs
                let costs = costs.into_iter()
                    .filter(|(region, _cost)|region_names_with_latency_data.contains_key(&region.name))
                    .map(|(region, cost)|{
                        let region = region_names_with_latency_data.get(&region.name).unwrap().clone();
                        (region, cost)
                    }).collect::<HashMap<Region, f64>>();

                (region, costs)
            }).collect::<NetworkCostMaps>();
            let network_ingress = network_ingress.into_iter().filter(|(region, _costs)|region_names_with_latency_data.contains_key(&region.name)).map(|(region, costs)|{
                let region = region_names_with_latency_data.get(&region.name).unwrap().clone();
                // Translate costs
                let costs = costs.into_iter()
                    .filter(|(region, _cost)|region_names_with_latency_data.contains_key(&region.name))
                    .map(|(region, cost)|{
                        let region = region_names_with_latency_data.get(&region.name).unwrap().clone();
                        (region, cost)
                    }).collect::<HashMap<Region, f64>>();

                (region, costs)
            }).collect::<NetworkCostMaps>();

            let region_names = region_names_with_latency_data;
            let regions = region_names.values().map(|r|r.clone()).collect_vec();

            (network_latency, region_names, regions, network_egress, network_ingress)
        } else {
            (LatencyMaps::new(), region_names, regions, network_egress, network_ingress)
        };

        assert!(region_names.len() > 0);
        
        let compatibility_checker = Self::load_compatibility_checker(network_latency.clone(), latency_slo);
        
        
        let object_stores = Loader::load_object_stores(object_store_file_path, &network_egress, &network_ingress, &region_names, object_store_pattern, object_store_list, Some(verbose));

        assert!(object_stores.len() > 0);

        // Verify that the regions of the object stores match
        let regions_hash_set: HashSet<Region> = HashSet::from_iter(regions.iter().map(|r|r.clone()));
        for object_store in &object_stores {
            assert!(regions_hash_set.contains(&object_store.region), "Object store {:?} has region {:?} which is not in the network", object_store, object_store.region);
        }
        
        // Load application regions
        let app_regions = regions.into_iter().map(|region|{
            
            let egress_cost = network_egress.get(&region).unwrap().clone();
            let ingress_cost = network_ingress.get(&region).unwrap().clone();
            
            ApplicationRegion{region, egress_cost, ingress_cost}
        }).collect_vec();

        // Verify that the object store's regions are in the source regions of the compatibility checker
        /* for object_store in &object_stores {
            for application_region in &app_regions {
                //println!("Checking source: {}, dest: {}", object_store.region.name, application_region.region.name);
                println!("Object store {:?} is compatible with application region {:?}: {}", object_store.region.name, application_region.region.name, compatibility_checker.is_compatible(object_store, application_region));
            }
        } */
        
        if verbose > 1 {
            println!("Number of object stores: {}, number of regions: {}", object_stores.len(), app_regions.len());
        }

        Loader {
            object_stores,
            app_regions,
            network_latency,
            compatibility_checker_slos: compatibility_checker
        }
    }

    pub fn load_latency(
        latency_file_path: &PathBuf,
        region_names: &HashMap<String, Region>,
        verbose: Option<i32>
    ) -> (LatencyMaps, HashMap<String, Region>) {

        let verbose = verbose.unwrap_or(0);
        
        let rdr2 = csv::Reader::from_path(latency_file_path).expect(format!("Failed to read latency file: {:?}", latency_file_path).as_str());
        let iter2: csv::DeserializeRecordsIntoIter<std::fs::File, LatencyRecordRaw> =
            rdr2.into_deserialize();

        let network_latency: LatencyMaps = iter2.filter_map(|x| {
                // Convert to LatencyRecord
                let mut l: LatencyRecord = x.unwrap().into();
                if region_names.contains_key(&l.src.name) && region_names.contains_key(&l.dest.name) {
                    // Translate region names to region IDs
                    l.src = region_names.get(&l.src.name).unwrap().clone();
                    l.dest = region_names.get(&l.dest.name).unwrap().clone();
                    Some(l)
                } else {
                    None
                }
            })
            // Translate into LatencyMap (HashMap<Region, f64>) of source regions
            .group_by(|x|x.src.clone()).into_iter().map(|(key, group)|{
                let latency_map: LatencyMap = group.map(|l|(l.dest, l.latency)).collect();
                (key, latency_map)
            }).collect();

        // Align considered regions with available latency data
        let missing_source_latency: HashSet<_> = region_names.values().filter(|r|!network_latency.contains_key(r)).collect();
        let missing_source_dest_latency =
            region_names.values().flat_map(|r| {
                region_names.values().filter(|r2|network_latency.contains_key(r) && ! network_latency.get(r).unwrap().contains_key(r2)).map(|x|(r.clone(), x.clone()))
            }).collect_vec();
        
        if missing_source_latency.len() > 0 && verbose > 0 {
            println!("Missing source latency for regions: {:?}", missing_source_latency);
        }
        if missing_source_dest_latency.len() > 0 && verbose > 0 {
            println!("Missing source/dest latency for regions: {:?}", missing_source_dest_latency);
        }

        let missing_dest_latency: HashSet<_> = missing_source_dest_latency.iter().map(|(_src, dest)|dest).collect();

        // Recompute region ids for regions with latency data
        let regions_with_latency: HashMap<String, Region> = region_names.into_iter()
            .filter(|(_name, r)|!missing_source_latency.contains(r) && !missing_dest_latency.contains(r))
            .map(|(n ,r)|(n.clone(), r.clone()))
            .sorted_by_key(|(name, _region)|name.clone())
            .enumerate()
            .map(|(i, (name, mut region))|{
                region.id = i as u16;
                (name, region)
            })
            .collect();

        // Update region IDs in network latency data
        let network_latency: LatencyMaps = network_latency.into_iter().map(|(src, latency_map)|{
            let src = regions_with_latency.get(&src.name).unwrap().clone();
            let latency_map: LatencyMap = latency_map.into_iter().map(|(dest, latency)|{
                let dest = regions_with_latency.get(&dest.name).unwrap().clone();
                (dest, latency)
            }).collect();
            (src, latency_map)
        }).collect();

        return (network_latency, regions_with_latency);
    }

    fn load_compatibility_checker(
        network_latency: LatencyMaps,
        network_slo: &Option<f64>,
    ) ->  Box<dyn CompatibilityChecker> {

        if let Some(network_slo) = network_slo {

            Box::new(CompatibilityCheckerNetworkSLOs::new(network_latency, *network_slo))
        } else {
            Box::new(DefaultCompatibilityChecker{})
        }
    }

    fn load_network(
        network_file_path: &PathBuf,
        region_pattern: Option<&str>,
        region_list: Option<Vec<Region>>,
        verbose: Option<i32>
    ) -> (NetworkCostMaps, Vec<Region>, HashMap<String, Region>) {
        
        let verbose = verbose.unwrap_or(0);

        let regions = if let Some(region_pattern) = region_pattern {

            let re = Regex::new(region_pattern).unwrap();
            
            let rdr2 = csv::Reader::from_path(network_file_path).expect(format!("Failed to read network file: {:?}", network_file_path).as_str());
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

            regions
        }
        else if let Some(region_list) = region_list {
            region_list
        }
        else {
            panic!("Must specify either region pattern or region list");
        };

        let region_names: HashMap<String, Region> = HashMap::from_iter(regions.iter().map(|r| (r.name.clone(), r.clone())));

        let rdr = csv::Reader::from_path(network_file_path).unwrap();
        let iter: csv::DeserializeRecordsIntoIter<std::fs::File, NetworkRecordRaw> =
            rdr.into_deserialize();

        let mut network_costs: NetworkCostMaps = iter
            .map(|r: Result<NetworkRecordRaw, csv::Error>| -> NetworkRecord { r.unwrap().into() })
            //.inspect(|r| println!("Raw network: {:?}", r))
            .filter(|r| region_names.contains_key(&r.src.name) && region_names.contains_key(&r.dest.name))
            //.inspect(|r| println!("Filtered network: {:?}", r))
            .fold(NetworkCostMaps::new(), |mut agg, e| {
                // Collect src/dest region in regions
                
                let src_region = region_names.get(&e.src.name).unwrap();

                let dest_region = region_names.get(&e.dest.name).unwrap();

                if src_region == dest_region {
                    debug_assert_eq!(e.src.name, e.dest.name);
                    debug_assert_eq!(e.cost, 0.0);
                }

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

        if regions.len() != unique && verbose > 0 {
            for r in &regions {
                println!("Region: {:?}", r);
            }
        }
        assert_eq!(regions.len(), unique);
        if region_pattern.is_some() {
            assert_eq!(min, 0);
            assert_eq!(max as usize, regions.len() - 1);
        }

        regions.iter().for_each(|r|{assert!(r.get_id() != u16::MAX, "Found region with id u16::MAX {:?}", r)});

        return (network_costs, regions, region_names);
    }

    fn load_object_stores(object_store_file_path: &PathBuf, egress_costs: &NetworkCostMaps, ingress_costs: &NetworkCostMaps, region_names: &HashMap<String, Region>, object_store_pattern: Option<&str>, object_store_list: Option<&Vec<String>>, verbose: Option<i32>) -> Vec<ObjectStore> {
        let verbose = verbose.unwrap_or(0);

        let object_store_regex = if object_store_list.is_none() {
            Regex::new(object_store_pattern.unwrap_or("")).unwrap()
        } else {
            // Do not filter by regex if object store list is specified
            Regex::new("XXXXXXX").unwrap()
        };

        let default_vec = vec![];
        let object_store_set:HashSet<&String> = HashSet::from_iter(object_store_list.unwrap_or(&default_vec));

        let rdr = csv::Reader::from_path(object_store_file_path).unwrap();
        let iter: csv::DeserializeRecordsIntoIter<std::fs::File, ObjectStoreStructRaw> =
            rdr.into_deserialize();

        let object_stores: Vec<ObjectStore> = iter
            .map(|x| x.unwrap().into())
            .filter(|r: &ObjectStoreStruct| {
                let full_name = format!("{}-{}", r.region.name, r.name);
                region_names.contains_key(&r.region.name)
                && (object_store_regex.is_match(&r.name) || object_store_set.contains(&full_name))
            })
            .map(|o|ObjectStore::new(o))
            // Combine object stores with identical names
            .fold(HashMap::new(), |mut agg, mut object_store| {
                let name = object_store.name.clone();
                let region_name = object_store.region.name.clone();
                let key = format!("{}-{}", name, region_name);

                // Set region with correct id
                if !region_names.contains_key(&region_name) && verbose > 0 {
                    println!("WARN: Region {:?} not found, skipping object store: {}", region_name, key);
                }
                else {
                    let region = region_names.get(&region_name).unwrap().clone();
                    object_store.region = region;
                    
                }
                let entry = agg.entry(key).or_insert(object_store.clone());
                entry.cost.merge(object_store.cost);

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
                    if verbose > 0 {
                        println!("WARN: No network costs for {:?}, in {:?}", x.name, x.region);
                    }
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

                // Verify that the object store has network costs to applications of its region corresponding to its put/get transfer costs
                let app_region = ApplicationRegion { region: x.region.clone(), egress_cost: egress_costs.get(&x.region).unwrap().clone(), ingress_cost: ingress_costs.get(&x.region).unwrap().clone() };
                let region_egress_costs = x.cost.get_egress_cost(&app_region, &x.region);
                debug_assert_eq!(region_egress_costs, x.cost.get_transfer);
                let region_ingress_costs = x.cost.get_ingress_cost(&app_region, &x.region);
                if region_ingress_costs != x.cost.put_transfer && verbose > 1 {
                    println!("WARN: Region ingress costs {:?} != put transfer costs {:?} for object store {:?}", region_ingress_costs, x.cost.put_transfer, x);
                }
                debug_assert_eq!(region_ingress_costs, x.cost.put_transfer);

                x
            })
            //.map(|x| ObjectStore::new(x))
            .collect_vec();

        assert!(object_stores.len() > 0);
        let min = object_stores.iter().map(|r|r.get_id()).min().unwrap();
        let max = object_stores.iter().map(|r|r.get_id()).max().unwrap();
        let unique = object_stores.iter().map(|r|r.get_id()).unique().collect_vec().len();

        if object_stores.len() != unique && verbose > 0 {
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
