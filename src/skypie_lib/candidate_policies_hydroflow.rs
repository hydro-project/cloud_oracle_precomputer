use hydroflow::{hydroflow_syntax, tokio_stream::{Stream}, bytes::{BytesMut, Bytes}, util::{deserialize_from_bytes, serialize_to_bytes}, futures::Sink};

use crate::{skypie_lib::{write_choice::WriteChoice, opt_assignments::opt_assignments, merge_policies::{AssignmentsRef, MergeIteratorRef}, decision::{Decision, DecisionRef}, object_store::ObjectStore, range::Range}, ApplicationRegion};

use super::monitor::MonitorMovingAverage;

pub type InputType = WriteChoice;
pub type OutputType = Decision;
//type CandidateReturnType = tokio_stream::wrappers::ReceiverStream<CandidateOutputType>;
//type CandidateReturnType = UnboundedReceiverStream<CandidateOutputType>;
pub type InputConnection = std::pin::Pin<Box<dyn Stream<Item = Result<BytesMut, std::io::Error>> + Send + Sync>>;
pub type OutputConnection = std::pin::Pin<Box<dyn Sink<Bytes, Error = std::io::Error> + Send + Sync>>;

pub fn candidate_policies_hydroflow<'a>(regions: &'static Vec<ApplicationRegion>, input: InputConnection, output: OutputConnection) -> hydroflow::scheduled::graph::Hydroflow
{

    let mut input_monitor = MonitorMovingAverage::new(1000);
    let mut output_monitor = MonitorMovingAverage::new(1000);
    

    let flow = hydroflow_syntax! {
        source_in = source_stream(input) -> map(|x| -> InputType {deserialize_from_bytes(x.unwrap()).unwrap()})
        -> map(|x: WriteChoice| Box::<WriteChoice>::new(x))
        -> flat_map(|w: Box::<WriteChoice>| {regions.iter().map(move |r: &'static ApplicationRegion| (w.clone(), r) )}
        ) -> inspect(|_|{
            input_monitor.add_arrival_time_now();
            input_monitor.print("Input:", Some(1000));
            /* if input_monitor.get_count() % 1000 == 0 {
                println!("Input: {}", input_monitor);
            } */
        });

        // Get optimal assignments of region r
        assignments = source_in -> flat_map(|(w, r): (Box::<WriteChoice>, &'static ApplicationRegion) | {
            opt_assignments(w.clone(), r).map(move |x| {(w.clone(), (r,x))} )
        });
        //-> inspect(|x|{println!("Assignments: {:?}", x);});

        // Convert to (upper bound, object store) pairs 
        //converted_assignments = assignments -> map(|(w, (r, (o, range)))| { (w, (r.clone(), (range.max, o)))});
        converted_assignments = assignments -> map(|(w, (r, (o, range))): (Box<WriteChoice>, (&'static ApplicationRegion, (ObjectStore, Range)))| { (w, (r, (range.max, o)))});
        //-> inspect(|x|{println!("Converted assignments: {:?}", x);});

        // / Merge iterator
        // Unpack MergeIterator to HydroFlow?

        // Collect assignments per write choice
        /* assignments_acc = converted_assignments -> fold_keyed(||{Assignments::new()}, |acc: &mut Assignments, (r, val)|{
            let v = acc.entry(r).or_insert(vec![]);
            v.push(val);
        }); */
        assignments_acc = converted_assignments -> fold_keyed(||{AssignmentsRef::new()}, |acc: &mut AssignmentsRef, (r, val)|{
            let v = acc.entry(r).or_insert(vec![]);
            v.push(val);
        });
        //-> inspect(|x|{println!("Accumulated assignments: {:?}", x);});

        // Create merge iterator
        merge_iterator = assignments_acc -> flat_map(|(write_choice, assignments):(Box<WriteChoice>, _)| {
            //MergeIterator::new(write_choice, assignments)
            MergeIteratorRef::new(write_choice, assignments)
        });
        //-> inspect(|x|{println!("Merge iterator: {:?}", x);});

        //merge_iterator -> for_each(|x| {let _ = output_send.send(x);});
        merge_iterator -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            if output_monitor.get_count() % 1000 == 0 {
                println!("Candidates out: {}", output_monitor);
            }
        })
        // Push into reduce_oracle
        -> map(|x: DecisionRef|{
            //x.write_choice
            
            let b = serialize_to_bytes(x.clone());
            //let des = deserialize_from_bytes::<Decision>(b.as_ref()).unwrap();
            //let eq = x == des;
            debug_assert_eq!(x, deserialize_from_bytes::<Decision>(b.as_ref()).unwrap());
            b
        })
        -> dest_sink(output);

    };

   /*  println!("Sending regions");
    // XXX: Properly async execution
    for region in regions {
        let _ = input_send.send((write_choice.clone(), region.clone()));
        //println!("Sent region: {:?}", region);
    }
    flow.run_available(); */

    return flow;

}

/* 
#[cfg(test)]
mod tests {
    use crate::skypie_lib::{object_store::{self, ObjectStore, ObjectStoreStruct, Cost}, write_choice::WriteChoice, region::Region, decision::Decision, read_choice::ReadChoice, network_record::NetworkCostMap, candidate_policies_hydroflow::candidate_policies_hydroflow};
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
} */