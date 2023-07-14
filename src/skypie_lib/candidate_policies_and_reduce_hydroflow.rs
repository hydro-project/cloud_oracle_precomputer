use std::{sync::atomic::{compiler_fence, Ordering::SeqCst}, collections::HashMap};

use hydroflow::{hydroflow_syntax, tokio_stream::{Stream}, bytes::{BytesMut, Bytes}, util::{deserialize_from_bytes}, futures::Sink, serde_json};
use itertools::Itertools;
use pyo3::{Python, PyAny, Py, types::PyModule};

use crate::{skypie_lib::{write_choice::WriteChoice, opt_assignments::opt_assignments, merge_policies::{MergeIterator}, decision::{Decision}, object_store::ObjectStore, self, reduce_oracle_hydroflow::BatcherMap, identifier::Identifier, monitor::{MonitorNOOP, MonitorMovingAverage}, output::OutputDecision}, ApplicationRegion, influx_logger::{InfluxLogger, InfluxLoggerConfig}, SkyPieLogEntry};

pub type InputType = WriteChoice;
pub type OutputType = Decision;
//type CandidateReturnType = tokio_stream::wrappers::ReceiverStream<CandidateOutputType>;
//type CandidateReturnType = UnboundedReceiverStream<CandidateOutputType>;
pub type InputConnection = std::pin::Pin<Box<dyn Stream<Item = Result<BytesMut, std::io::Error>> + Send + Sync>>;
pub type OutputConnection = std::pin::Pin<Box<dyn Sink<Bytes, Error = std::io::Error> + Send + Sync>>;

pub fn candidate_policies_reduce_hydroflow<'a>(regions: &'static Vec<ApplicationRegion>, input: InputConnection, batch_size: usize, experiment_name: String, output_candidates_file_name: String, output_file_name: String, logger: InfluxLogger) -> hydroflow::scheduled::graph::Hydroflow
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

    let logger_sink = Box::pin(logger.into_sink::<SkyPieLogEntry>());

    let mut input_monitor = MonitorNOOP::new(1000); //MonitorNOOP::new(0); //MonitorMovingAverage::new(1000);
    //let mut output_monitor = MonitorMovingAverage::new(1000); //MonitorNOOP::new(0);

    let module = "";
    let fun_name = "redundancy_elimination";
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
    let batch_logging_frequency = Some(1);
    let mut reduce_output_monitor =  MonitorMovingAverage::new(1000); // MonitorNOOP::new(0);
    let optimal_log_interval = Some(1000);

    let flow = hydroflow_syntax! {
        source_in = source_stream(input) -> map(|x| -> InputType {deserialize_from_bytes(x.unwrap()).unwrap()})
        -> inspect(|_|{
            input_monitor.add_arrival_time_now();
            input_monitor.print("Input:", Some(1000));
        })
        -> demux(|v, var_args!(out, time)| {
            let now = std::time::Instant::now();
            compiler_fence(SeqCst);
            time.give(now);
            out.give(v);
        }
        );

        logger_sink = union() -> dest_sink(logger_sink);

        /* source_parsed = source_in[out] -> map(|x: WriteChoice| Box::<WriteChoice>::new(x))
        -> flat_map(|w: Box::<WriteChoice>| { regions.iter().map(move |r: &'static ApplicationRegion| (w.clone(), r) )})
        //-> inspect(|(_, r)|{assert_ne!(r.get_id(), u16::MAX); println!("Region: {}", r.get_id());})
        -> inspect(|_|{
            input_monitor.add_arrival_time_now();
            input_monitor.print("Input:", Some(1000));
        });
        //-> inspect(|(_, r)|{println!("In assignments: {}", r.get_id());});

        // Get optimal assignments of region r
        assignments = source_parsed -> flat_map(|(w, r): (Box::<WriteChoice>, &'static ApplicationRegion) | {
            opt_assignments(w.clone(), r).map(move |x| {(w.clone(), (r,x))} )
        });
        //-> inspect(|(_, (r, _))|{println!("Opt assignment of region {}", r.get_id());});

        // Above is correct up to this point #############################

        // Convert to (upper bound, object store) pairs 
        converted_assignments = assignments
        -> map(|(w, (r, (o, range))): (Box<WriteChoice>, (&'static ApplicationRegion, (ObjectStore, Range)))| -> (Box<WriteChoice>, (&'static ApplicationRegion, (f64, ObjectStore))) { (w, (r, (range.max, o)))})
        -> inspect(|(_, (r, _)): &(_,(&'static ApplicationRegion,_))| {debug_assert_ne!(r.get_id(), u16::MAX);});

        assignments_acc = converted_assignments -> fold_keyed::<'tick>(||{AssignmentsRef::new()}, |acc: &mut AssignmentsRef, (r, val)|{
            let v = acc.entry(r).or_insert(vec![]);
            v.push(val);
        })
        -> inspect(|(_, assignments)|{
            debug_assert_eq!(assignments.len(), regions.len());
            for (r, _) in assignments.iter() {
                debug_assert_ne!(r.get_id(), u16::MAX);
            }
        });

        // Create merge iterator
        candidates = assignments_acc -> flat_map(|(write_choice, assignments):(Box<WriteChoice>, _)| {
            for (r, _) in assignments.iter() {
                debug_assert_ne!(r.get_id(), u16::MAX);
            }
            //MergeIterator::new(write_choice, assignments)
            MergeIteratorRef::new(write_choice, assignments)
        })
        -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Candidates out: ", Some(1000));
        })
        -> tee(); */

        // Non-hydro version
        candidates = source_in[out] -> map(|write_choice| {

            let write_choice = Box::<WriteChoice>::new(write_choice);
            //let mut i = 0; // for debugging
            let assignments = regions.iter().map(|r|{
                // Get optimal assignments of region r
                let assignments = opt_assignments(write_choice.clone(), r);
                // Convert to (upper bound, object store) pairs 
                let assignments = assignments.map(|(o, range)|(range.max, o));
                // XXX: For debugging insert fixed values
                //let assignments = assignments.enumerate().map(|(i, (o, _range))|((i) as f64, o));
                //let assignments = assignments.map(|(o, _range)|{i += 1; (i as f64, o)});

                // Collect into vector
                return (r.clone(), assignments.collect::<Vec<(f64, ObjectStore)>>());
            });
            /* .inspect(|(_, assignments)|{
                debug_assert_eq!(assignments.len(), write_choice.object_stores.len());
            }); */


            // Merge assignments per region to candidate policies
            // XXX: Materializing here is not necessary
            // In HyrdoFlow, use stream of (region, upper bound, object store), then search min per region and then merge?
            let assignments = HashMap::from_iter(assignments);

            // XXX: Debug output for optimal assignments
            /* write_choice.object_stores.iter().for_each(|o|println!("Object store: {}-{}", o.region.name, o.name));
            assignments.iter().for_each(|(k, v)|println!("Region {}: {:?}", k.region.name, v.iter().map(|(ub, o)|format!("({}, {}-{})", ub, o.region.name, o.name)).collect::<Vec<String>>())); */

            // Verify assignments
            /* debug_assert_eq!(assignments.len(), regions.len());
            for (r, ass) in assignments.iter() {
                debug_assert_ne!(r.get_id(), u16::MAX);
                debug_assert_eq!(ass.len(), write_choice.object_stores.len());
            } */

            //XXX: Debug MergeIterator
            /* let m =  MergeIterator::new(write_choice.clone(), assignments.clone());
            let candidates = m.collect_vec();
            assert_eq!(candidates.len(), regions.len() + 1);
            println!("Candidates of region: {}", candidates.len());

            candidates_count += candidates.len();
            println!("Candidates: {}", candidates_count); */

            return MergeIterator::new(write_choice, assignments);
        }) -> flatten() -> tee();

        // Find the bug above

        // Output candidates
        candidates
        -> map(|d: Decision| -> OutputDecision {d.into()})
        //-> map(|d: DecisionRef| -> OutputDecision {d.into()})
        -> map(|d|serde_json::to_string(&d).unwrap())
        -> dest_file(output_candidates_file_name, false);

         // Measure candidate cycle time here
         candidates
         -> map(|_: _| (1, std::time::Instant::now()))
         -> reduce::<'tick>(|acc: &mut (usize, std::time::Instant), (len, start_time)|{
             acc.0 = acc.0 + len;
             acc.1 = acc.1.max(start_time);
            })
        -> [1]measurement_candidates;
        measurement_candidates = zip(); //cross_join::<'tick, HalfMultisetJoinState>();
        source_in[time] -> reduce::<'tick>(|acc: &mut std::time::Instant, e| {*acc = (*acc).min(e)}) -> enumerate() -> [0]measurement_candidates;
        measurement_candidates
         -> map(|((i, start_time), (len, end_time))| (end_time.duration_since(start_time).as_secs_f64(), i, len))
         -> map(|(t, _i, len)|SkyPieLogEntry::new(t, len as u64, "candidates".to_string(), experiment_name.clone()))
         //-> inspect(|x|{println!("{}: {:?}", context.current_tick(), x);})
         -> logger_sink;

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
        reduce_input = candidates -> map(|x: Decision| x); //-> map(|x: DecisionRef| x.into());

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
            reduce_batch_monitor.print("Batches:", batch_logging_frequency);
        })
        -> demux(|v, var_args!(out, time)| {
            let now = std::time::Instant::now();
            compiler_fence(SeqCst);
            time.give(now);
            out.give(v);
        });

        // Redundancy elimination via python
        optimal = batches[out] -> map(|decisions| {
            // Convert batch of decisions to numpy array
            let py_array = Decision::to_inequalities_numpy(&decisions);


            Python::with_gil(|py| {
                type T = Vec<usize>;
    
                // Computing optimal decisions by row IDs in vector
                let py_res = fun.call(py, (py_array,), None).unwrap();
                let res: T = py_res.extract(py).unwrap();

                //println!("Optimal decisions: ({}) {:?}", res.len(), res);
                
                // Extract optimal decisions by ids in res
                let mut optimal = Vec::with_capacity(res.len());
                for id in res {
                    optimal.push(decisions[id].clone());
                }

                optimal
                //DecisionsExtractor::new(decisions, res)
            })
        })
        -> tee();

        //-> flatten();

        optimal -> flatten() -> map(|d: Decision| -> OutputDecision {d.into()})
        //-> map(|d: DecisionRef| -> OutputDecision {d.into()})
        -> map(|d|serde_json::to_string(&d).unwrap())
        -> dest_file(output_file_name, false);

        //-> py_run(args.code, args.module, args.function)
        //optimal_tee = optimal -> tee();
        optimal -> flatten() -> for_each(|_x|{
            reduce_output_monitor.add_arrival_time_now();
            reduce_output_monitor.print("Optimal:", optimal_log_interval);
        });

        measurement = zip();
        batches[time] -> enumerate() -> [0]measurement;
        optimal -> map(|x|(x.len(), std::time::Instant::now())) -> [1]measurement;
        measurement -> map(|((epoch, start_time), (len, end_time))|
            (len, end_time.duration_since(start_time).as_secs_f64(), epoch))
        //-> inspect(|x|{println!("{}: {:?}", context.current_tick(), x);})
        -> reduce::<'tick>(|acc: &mut (usize, f64, usize), (len, duration, epoch)|{
             acc.0 = acc.0 + len;
             acc.1 = acc.1 + duration;
             acc.2 = acc.2.max(epoch);
            })
        -> map(|(len, duration, _epoch)|SkyPieLogEntry::new(duration, len as u64, "optimal".to_string(), experiment_name.clone()))
        //-> inspect(|x|{println!("{}: {:?}", context.current_tick(), x);})
        -> logger_sink;

    };

    //eprintln!("{}", flow.meta_graph().unwrap().to_mermaid());

    return flow;

}
