use std::{collections::HashMap, process::Output};

use hydroflow::{hydroflow_syntax, tokio_util, tokio_stream::{wrappers::UnboundedReceiverStream, self}};

use crate::{write_choice::WriteChoice, region::{Region, self}, opt_assignments::opt_assignments, merge_policies::{MergeIterator, Assignments}, object_store::ObjectStore, range::Range, decision::Decision};

pub(crate) fn candidate_policies(write_choice: WriteChoice, regions: &Vec<Region>) -> MergeIterator {
    let assignments = regions.iter().map(|r|{
        // Get optimal assignments of region r
        let assignments = opt_assignments(write_choice.clone(), r);
        // Convert to (upper bound, object store) pairs 
        let assignments = assignments.map(|(o, range)|(range.max, o));
        // Collect into vector
        return (r.clone(), assignments.collect::<Vec<(f64, ObjectStore)>>());
    });


    // Merge assignments per region to candidate policies
    // XXX: Materializing here is not necessary
    // In HyrdoFlow, use stream of (region, upper bound, object store), then search min per region and then merge?
    let assignments = HashMap::from_iter(assignments);
    return MergeIterator::new(write_choice, assignments)
}

#[cfg(test)]
mod tests {
    use crate::{object_store::{self, ObjectStore, ObjectStoreStruct, Cost}, write_choice::WriteChoice, region::Region, decision::Decision, read_choice::ReadChoice, network_record::NetworkCostMap};
    extern crate test;
    use hydroflow::futures::{StreamExt, executor::block_on};
    use test::Bencher;

    use super::candidate_policies;

    #[test]
    fn test_candidate_policies() {
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

        let region = vec![Region{name: "0".to_string()}];
        let res: Vec<Decision> = candidate_policies(write_choice.clone(), &region).collect();
        
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].write_choice, write_choice);
        assert_eq!(res[0].read_choice, ReadChoice::from_iter(vec![(region[0].clone(), o2.clone())]));
        assert_eq!(res[1].write_choice, write_choice);
        assert_eq!(res[1].read_choice, ReadChoice::from_iter(vec![(region[0].clone(), o1.clone())]));
        
    }

    #[bench]
    fn bench_candidate_policies_native(b: &mut Bencher) {

        let write_choice_size = 5;
        let read_choice_size = 200;

        let read_choice_size = test::black_box(read_choice_size);
        let regions: Vec<super::Region> = (0..read_choice_size).map(|x| super::Region{name: x.to_string()}).collect();

        let write_choice_size = test::black_box(write_choice_size);
        let mut cost = Cost::new(1.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(regions.iter().map(|r| (r.clone(), 1.0)));
        cost.add_egress_costs(egress_cost);
        let object_stores: Vec<object_store::ObjectStore> = (0..write_choice_size)
            .map(|x| object_store::ObjectStoreStruct{name: x.to_string(), id:x, region: Region { name: "regionX".to_string() }, cost: cost.clone()})
            .map(|x| ObjectStore::new(x))
            .collect();


        let write_choice = super::WriteChoice {
            object_stores: object_stores,
        };

        println!("Benchmarking candidate_policies with {} write choices and {} read choices", write_choice_size, read_choice_size);
        b.iter(|| {
            let res: Vec<Decision> = candidate_policies(write_choice.clone(), &regions).collect();
            
            return  res;
        });
    }

}