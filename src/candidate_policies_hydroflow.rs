use std::{collections::HashMap, process::Output};

use hydroflow::{hydroflow_syntax, tokio_util, tokio_stream::{wrappers::UnboundedReceiverStream, self}};

use crate::{write_choice::WriteChoice, region::{Region, self}, opt_assignments::opt_assignments, merge_policies::{MergeIterator, Assignments}, object_store::ObjectStore, range::Range, decision::Decision};

type CandidateInputType = (WriteChoice, Region);
type CandidateOutputType = Decision;
//type CandidateReturnType = tokio_stream::wrappers::ReceiverStream<CandidateOutputType>;
type CandidateReturnType = UnboundedReceiverStream<CandidateOutputType>;
pub(crate) fn candidate_policies_hydroflow(write_choice: WriteChoice, regions: &Vec<Region>) -> CandidateReturnType {

    // Input into hydroflow
    let (input_send, input_recv) = hydroflow::util::unbounded_channel::<CandidateInputType>();
    
    // Output from hydroflow
    // XXX: Use proper channel
    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<CandidateOutputType>();
    /* let (output_send, output_recv) = tokio::sync::mpsc::channel::<CandidateOutputType>(1024);
    // `PollSender` adapts the send half of the bounded channel into a `Sink`.
    let output_send = tokio_util::sync::PollSender::new(output_send);
    // Wrap output into a stream
    //let output_recv = tokio_stream::wrappers::ReceiverStream::new(output_recv); */

    let mut flow = hydroflow_syntax! {
        
        source_in = source_stream(input_recv);
        // Get optimal assignments of region r
        assignments = source_in -> map(|(w, r)| {opt_assignments(w.clone(), &r).map(move |x| (w.clone(), (r.clone(),x)))}) -> flatten();
        //-> inspect(|x|{println!("Assignments: {:?}", x);});

        // Convert to (upper bound, object store) pairs 
        //let assignments = assignments.map(|(o, range)|(range.max, o));
        converted_assignments = assignments -> map(|(write_choice, (r, (o, range)))|(write_choice, (r, (range.max, o))));
        //-> inspect(|x|{println!("Converted assignments: {:?}", x);});

        // / Merge iterator
        // Unpack MergeIterator to HydroFlow?

        // Collect assignments per write choice
        assignments_acc = converted_assignments -> fold_keyed(||{Assignments::new()}, |acc: &mut Assignments, (r, val)|{
            let v = acc.entry(r).or_insert(vec![]);
            v.push(val);
        });
        //-> inspect(|x|{println!("Accumulated assignments: {:?}", x);});

        // Create merge iterator
        merge_iterator = assignments_acc -> flat_map(|(write_choice, assignments)| {
            MergeIterator::new(write_choice, assignments)
        });
        //-> inspect(|x|{println!("Merge iterator: {:?}", x);});

        //merge_iterator -> dest_sink(output_send);
        merge_iterator -> for_each(|x| {
            let _ = output_send.send(x);
        });

    };

    println!("Sending regions");
    // XXX: Properly async execution
    for region in regions {
        let _ = input_send.send((write_choice.clone(), region.clone()));
        //println!("Sent region: {:?}", region);
    }
    flow.run_available();

    return output_recv;

}


#[cfg(test)]
mod tests {
    use crate::{object_store::{self, ObjectStore, ObjectStoreStruct, Cost}, write_choice::WriteChoice, region::Region, decision::Decision, read_choice::ReadChoice, network_record::NetworkCostMap, candidate_policies_hydroflow::candidate_policies_hydroflow};
    extern crate test;
    use hydroflow::futures::{StreamExt, executor::block_on};
    use test::Bencher;

    #[tokio::test]
    async fn test_candidate_policies_hydroflow() {
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
        let res: Vec<Decision> = candidate_policies_hydroflow(write_choice.clone(), &region).collect().await;
        
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].write_choice, write_choice);
        assert_eq!(res[0].read_choice, ReadChoice::from_iter(vec![(region[0].clone(), o2.clone())]));
        assert_eq!(res[1].write_choice, write_choice);
        assert_eq!(res[1].read_choice, ReadChoice::from_iter(vec![(region[0].clone(), o1.clone())]));
        
    }

    #[bench]
    fn bench_candidate_policies_hydroflow(b: &mut Bencher) {

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

        b.iter(|| {
            block_on(async {
                let res: Vec<Decision> = candidate_policies_hydroflow(write_choice.clone(), &regions).collect().await;
                
                return  res;
            })
        });
    }
}