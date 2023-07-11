use hydroflow::{hydroflow_syntax, tokio_stream::{Stream}, bytes::{BytesMut, Bytes}, util::{deserialize_from_bytes}, futures::Sink};
use itertools::Itertools;
use pyo3::{Python, PyAny, Py, types::PyModule};

use crate::{skypie_lib::{write_choice::WriteChoice, opt_assignments::opt_assignments, merge_policies::{AssignmentsRef, MergeIteratorRef}, decision::{Decision, DecisionRef, DecisionsExtractor}, object_store::ObjectStore, range::Range, self, reduce_oracle_hydroflow::BatcherMap, identifier::Identifier, monitor::MonitorNOOP}, ApplicationRegion};

use super::monitor::MonitorMovingAverage;

pub type InputType = WriteChoice;
pub type OutputType = Decision;
//type CandidateReturnType = tokio_stream::wrappers::ReceiverStream<CandidateOutputType>;
//type CandidateReturnType = UnboundedReceiverStream<CandidateOutputType>;
pub type InputConnection = std::pin::Pin<Box<dyn Stream<Item = Result<BytesMut, std::io::Error>> + Send + Sync>>;
pub type OutputConnection = std::pin::Pin<Box<dyn Sink<Bytes, Error = std::io::Error> + Send + Sync>>;

pub fn candidate_policies_reduce_hydroflow<'a>(regions: &'static Vec<ApplicationRegion>, input: InputConnection, batch_size: usize) -> hydroflow::scheduled::graph::Hydroflow
{
    {
        // Validate application regions
        let min = regions.iter().map(|r|r.get_id()).min().unwrap();
        let max = regions.iter().map(|r|r.get_id()).max().unwrap();
        let unique = regions.iter().map(|r|r.get_id()).unique().collect_vec().len();

        debug_assert_eq!(regions.len(), unique);
        debug_assert_eq!(min, 0);
        debug_assert_eq!(max as usize, regions.len() - 1);

        for r in regions {
            debug_assert_ne!(r.get_id(), u16::MAX);
            debug_assert_ne!(r.region.get_id(), u16::MAX);
        }
    
    }

    let mut input_monitor = MonitorMovingAverage::new(1000);
    let mut output_monitor = MonitorMovingAverage::new(1000);

    let module = "";
    let fun_name = "redundancy_elimination";
    //let fun_name = "redundancy_elimination_dummy";
    // Read python code from file at compile time in current directory
    let code = include_str!("python_redundancy_bridge.py");
    let fun = Python::with_gil(|py| {

        let fun: Py<PyAny> =
            //PyModule::import(py, module)
            PyModule::from_code(py, code, "", module)
            .unwrap()
            .getattr(fun_name)
            .unwrap()
            .into();
        fun
    });
    let fun: &Py<PyAny> = &*Box::leak(Box::new(fun));

    type Input = skypie_lib::candidate_policies_hydroflow::OutputType;
    let mut batcher = BatcherMap::<Input>::new(batch_size);

    let mut reduce_input_monitor = MonitorNOOP::new(1000); //MonitorMovingAverage::new(1000);
    let mut reduce_batch_monitor = MonitorNOOP::new(1000); //MonitorMovingAverage::new(1000);
    let mut reduce_output_monitor = MonitorMovingAverage::new(1000);
    let optimal_log_interval = Some(1000);
    

    let flow = hydroflow_syntax! {
        source_in = source_stream(input) -> map(|x| -> InputType {deserialize_from_bytes(x.unwrap()).unwrap()})
        -> map(|x: WriteChoice| Box::<WriteChoice>::new(x))
        -> flat_map(|w: Box::<WriteChoice>| { regions.iter().map(move |r: &'static ApplicationRegion| (w.clone(), r) )})
        //-> inspect(|(_, r)|{assert_ne!(r.get_id(), u16::MAX); println!("Region: {}", r.get_id());})
        -> inspect(|_|{
            input_monitor.add_arrival_time_now();
            input_monitor.print("Input:", Some(1000));
        });

        // Get optimal assignments of region r
        assignments = source_in -> flat_map(|(w, r): (Box::<WriteChoice>, &'static ApplicationRegion) | {
            opt_assignments(w.clone(), r).map(move |x| {(w.clone(), (r,x))} )
        });
        //-> inspect(|(_, (r, _))|{println!("Assignments: {}", r.get_id());});

        // Convert to (upper bound, object store) pairs 
        converted_assignments = assignments
        -> map(|(w, (r, (o, range))): (Box<WriteChoice>, (&'static ApplicationRegion, (ObjectStore, Range)))| { (w, (r, (range.max, o)))})
        -> inspect(|(_, (r, _))|{assert_ne!(r.get_id(), u16::MAX);});

        assignments_acc = converted_assignments -> fold_keyed(||{AssignmentsRef::new()}, |acc: &mut AssignmentsRef, (r, val)|{
            let v = acc.entry(r).or_insert(vec![]);
            v.push(val);
        })
        -> inspect(|(_, assignments)|{
            assert_eq!(assignments.len(), regions.len());
            for (r, _) in assignments.iter() {
                assert_ne!(r.get_id(), u16::MAX);
            }
        });

        // Create merge iterator
        candidates = assignments_acc -> flat_map(|(write_choice, assignments):(Box<WriteChoice>, _)| {
            for (r, _) in assignments.iter() {
                debug_assert_ne!(r.get_id(), u16::MAX);
            }
            //MergeIterator::new(write_choice, assignments)
            MergeIteratorRef::new(write_choice, assignments)
        }) -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Candidates out: ", Some(1000));
        });

        // Push into reduce_oracle
        /* candidates -> map(|x: DecisionRef|{
            //x.write_choice
            
            let b = serialize_to_bytes(x.clone());
            //let des = deserialize_from_bytes::<Decision>(b.as_ref()).unwrap();
            //let eq = x == des;
            debug_assert_eq!(x, deserialize_from_bytes::<Decision>(b.as_ref()).unwrap());
            b
        })
        -> dest_sink(output); */

        // XXX: Materializing decisions here, since putting lifetime into the Batcher is difficult
        reduce_input = candidates -> map(|x: DecisionRef| x.into());

        //input = source_stream(input_recv) -> map(|x| -> Input {deserialize_from_bytes(x.unwrap()).unwrap()});
        batches = reduce_input -> inspect(|_|{
            reduce_input_monitor.add_arrival_time_now();
            reduce_input_monitor.print("Candidates in:", Some(1000));
        })
        // Collect batch of decisions, batcher returns either None or Some(batch)
        // Filter_map drops None values
        // XXX: Use hydro's batch operator. But it has to have sufficient items, about batch size!
        -> filter_map(|x: Input|{
            return batcher.add(x)
        })
        -> inspect(|_|{
            reduce_batch_monitor.add_arrival_time_now();
            reduce_batch_monitor.print("Batches:", None);
        });

        // Redundancy elimination via python
        optimal = batches -> map(|decisions| {
            // Convert batch of decisions to numpy array
            let py_array = Decision::to_inequalities_numpy(&decisions);


            Python::with_gil(|py| {
                type T = Vec<usize>;
    
                // Computing optimal decisions by row IDs in vector
                let py_res = fun.call(py, (py_array,), None).unwrap();
                let res: T = py_res.extract(py).unwrap();

                //println!("Optimal decisions: ({}) {:?}", res.len(), res);
                
                // Extract optimal decisions by ids in res
                /* let mut optimal = Vec::with_capacity(res.len());
                for id in res {
                    optimal.push(decisions[id].clone());
                }

                optimal */
                DecisionsExtractor::new(decisions, res)
            })
        })
        -> flatten();

        //-> py_run(args.code, args.module, args.function)
        optimal -> for_each(|_x|{
            reduce_output_monitor.add_arrival_time_now();
            reduce_output_monitor.print("Optimal:", optimal_log_interval);
        });

    };

    return flow;

}