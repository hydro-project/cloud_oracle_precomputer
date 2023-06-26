use crate::{
    object_store::{ObjectStore, ObjectStoreStruct},
    range::Range,
    region::Region, write_choice::WriteChoice,
};
use itertools::Itertools;
use std::{collections::HashMap};

pub(crate) fn opt_assignments(write_choice: WriteChoice, region: &Region) -> std::vec::IntoIter<(ObjectStore, Range)> {
    
    debug_assert_ne!(write_choice.object_stores.len(), 0);

    let compat = write_choice.object_stores
        .into_iter()
        .filter(|s| s.is_compatible_with(&region));
    // Generate combinations of object stores
    let c = compat
        .tuple_combinations();
        // Debug output
        //.inspect(|x:&(ObjectStore, ObjectStore)| {println!("Combinations of object stores after map: {:?}-{:?}", x.0.name, x.1.name)});

    // Generate intersections of object stores
    let intersection = c
        .map(|(o, p)| ObjectStoreStruct::intersect(o, p, &region));
        /* .inspect(|[(o1, r1), (o2, r2)]| {
            println!("Intersections of object stores: {:?}-{:?}={:?}-{:?}", o1.region, o1.name, r1.min, r1.max);
            println!("Intersections of object stores: {:?}-{:?}={:?}-{:?}", o2.region, o2.name, r2.min, r2.max);
        }); */
        
    // Aggregate range by object store, fold into hashmap
    let agg = intersection.flatten().fold(HashMap::<ObjectStore, Range>::new(), |mut agg, (object_store, range)| {
        {
            // Insert or update entry for intersection 0
            let e = agg.entry(object_store).or_insert(Range::new());
            e.merge(&range);
        }
        agg
    });
    let opt_assignments = agg.into_iter().filter(|(_, r)| r.non_empty());
        //.inspect(|(o, r)| println!("Optimal assignment: {:?}-{:?}={:?}-{:?}", o.region, o.name, r.min, r.max));

    // XXX: Do proper into iter without intermediate vector
    opt_assignments.collect::<Vec<(ObjectStore, Range)>>().into_iter()
}

#[cfg(test)]
mod tests {
    use std::{f64::NEG_INFINITY, f64::INFINITY};

    use itertools::Itertools;

    use crate::{object_store::{ObjectStore, ObjectStoreStruct, Cost}, region::Region, write_choice::WriteChoice, range::Range, network_record::NetworkCostMap};

    use super::opt_assignments;


    #[test]
    fn test_opt_assignments() {
        let mut cost1 = Cost::new(10.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(vec![(Region{name:"0".to_string()}, 1.0)]);
        cost1.add_egress_costs(egress_cost);
        let o1 = ObjectStore::new(ObjectStoreStruct{id: 0, cost: cost1, region: Region { name: "".to_string()}, name: "".to_string()});
        
        let mut cost2 = Cost::new(2.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(vec![(Region{name:"0".to_string()}, 2.0)]);
        cost2.add_egress_costs(egress_cost);
        let o2 = ObjectStore::new(ObjectStoreStruct{id: 1, cost: cost2, region: Region { name: "".to_string()}, name: "".to_string()});

        let write_choice = WriteChoice{
            object_stores: vec![
                o1.clone(),
                o2.clone()
            ]
        };

        let region = Region{name: "0".to_string()};
        let res: Vec::<(ObjectStore, Range)> = opt_assignments(write_choice, &region).sorted_by(|(a,_), (b,_)|Ord::cmp(&a.id, &b.id)).collect();
        res.iter().for_each(|(id, r)| println!("{:?}: ({:?}, {:?})", id, r.min, r.max));
        
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], (o1, Range{min: 8.0, max: INFINITY}));
        assert_eq!(res[1], (o2, Range{min: NEG_INFINITY, max: 8.0}));
    }
}
