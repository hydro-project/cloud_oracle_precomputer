use itertools::Itertools;

use crate::decision::Decision;
use crate::object_store::ObjectStore;
use crate::candidate_policies::candidate_policies;
use crate::reduce_oracle::Batcher;
use crate::region::Region;
use crate::write_choice::WriteChoice;


pub fn skypie_precomputation(regions: Vec<Region>, object_stores: Vec<ObjectStore>, replication_factor: usize, batch_size: usize) -> Vec<Decision> {
    
    println!("Regions {:?}", regions.len());
    println!("Object stores {:?}", object_stores.len());
    println!("Replication factor {:?}", replication_factor);

    let mut combination_count = 0;
    let mut enumeration_count = 0;

    // 1. Enumerate all possible write choices
    // Stage 1 for parallelism
    let write_choices = object_stores.iter().map(|object_store|{
        // Stage 2 to cover rest of combinations
        object_stores.iter()
            .filter(move |x| *x != object_store)
            .map(|x| x.clone())
            .combinations(replication_factor-1)
            .map(|vec_c|{
                let mut vec_c = vec_c;
                // XXX: should be able to move here instead
                vec_c.push(object_store.clone());
                vec_c
        })
    })
    .flatten()
    .inspect(|_x|{
        //println!("Write choice {:?}", x);
        if combination_count % 100 == 0 {
            println!("Storage choices {:?}", combination_count);
        }
        combination_count+=1;
    });

    // 2. Compute candidate decisions by optimal read choices per write choice
    let all_candidates = write_choices.map(|x| {
        let write_choice = WriteChoice { object_stores: x };
        let candidates  = candidate_policies(write_choice, &regions);
        candidates
    }).flatten()
    .inspect(|_|{
        if enumeration_count % 100 == 0 {
            println!("Candidate choices {:?}", enumeration_count);
        }
        enumeration_count+=1;
    });
    //.inspect(|x| println!("Candidate {:?}", x));

    // 4. Reduce batched candidate decisions
    // Pull in a batch of candidates
    let batches = Batcher::new(batch_size, all_candidates);
    
    // Consume a batch of decisions
    // Dummy
    let dummy_selectivity = 0;
    let reduced = batches.into_iter().map(|mut batch| {
        batch.split_off(test::black_box(dummy_selectivity))
    }).flatten();
    
    // 5. Write out final decisions
    return reduced.collect();
}

#[cfg(test)]
mod tests {
    use crate::{object_store::{ObjectStore, ObjectStoreStruct, Cost}, write_choice::WriteChoice, region::Region, decision::Decision, read_choice::ReadChoice, skypie_precomputation::skypie_precomputation, network_record::NetworkCostMap};
    extern crate test;
    //use test::Bencher;

    #[test]
    fn test_skypie_precomputation() {
        let replication_factor = 2;
        let batch_size = 2;

        let mut cost1 = Cost::new(10.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(vec![(Region{name:"0".to_string()}, 1.0)]);
        cost1.add_egress_costs(egress_cost);
        let o1 = ObjectStore::new(ObjectStoreStruct{id: 0, cost: cost1, region: Region { name: "".to_string()}, name: "".to_string()});
        
        let mut cost2 = Cost::new(2.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(vec![(Region{name:"0".to_string()}, 2.0)]);
        cost2.add_egress_costs(egress_cost);
        let o2 = ObjectStore::new(ObjectStoreStruct{id: 1, cost: cost2, region: Region { name: "".to_string()}, name: "".to_string()});

        let object_stores = vec![
            o1.clone(),
            o2.clone()
        ];

        let regions = vec![Region{name:"0".to_string()}];
        let res: Vec<Decision> = skypie_precomputation(regions.clone(), object_stores, replication_factor, batch_size);
        
        let write_choice = WriteChoice{
            object_stores: vec![
                o1.clone(),
                o2.clone()
            ]
        };
        
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].write_choice, write_choice);
        assert_eq!(res[0].read_choice, ReadChoice::from_iter(vec![(regions[0].clone(), o2.clone())]));
        assert_eq!(res[1].write_choice, write_choice);
        assert_eq!(res[1].read_choice, ReadChoice::from_iter(vec![(regions[0].clone(), o1.clone())]));
        
    }
}