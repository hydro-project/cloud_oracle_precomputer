use std::{collections::HashMap, path::Path};

use hydroflow::{hydroflow_syntax, tokio_stream::Stream, bytes::{BytesMut, Bytes}, futures::Sink, util::{deserialize_from_bytes, serialize_to_bytes}};
use itertools::Itertools;
use pyo3::{Python, PyAny, Py, types::{PyModule, PyDict}};
use skypie_proto_messages::ProtobufFileSink;

use crate::{
    write_choice::WriteChoice,
    decision::Decision,
    object_store::ObjectStore,
    BatcherMap,
    identifier::Identifier,
    monitor::{MonitorNOOP, MonitorMovingAverage},
    ApplicationRegion,
    compatibility_checker::CompatibilityChecker, log_entry::SkyPieLogEntryType, opt_assignments::opt_assignments, merge_policies::MergeIterator, Tombstone
};

pub type InputType = WriteChoice;
pub type OutputType = Decision;
pub type InputConnection = std::pin::Pin<Box<dyn Stream<Item = Result<BytesMut, std::io::Error>> + Send + Sync>>;
pub type OutputConnection = std::pin::Pin<Box<dyn Sink<Bytes, Error = std::io::Error> + Send + Sync>>;

pub fn candidate_policies_reduce_hydroflow(regions: &'static Vec<ApplicationRegion>, input: InputConnection, batch_size: usize, _experiment_name: String, output_candidates_file_name: String, output_file_name: String, object_store_id_map: HashMap<u16, ObjectStore>, time_sink: OutputConnection, done_sink: OutputConnection, worker_id: usize, optimizer: Option<String>, use_clarkson: bool, compatibility_checker_slos: Box<dyn CompatibilityChecker>, count_sink: OutputConnection) -> hydroflow::scheduled::graph::Hydroflow
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

    let no_dimensions = if use_clarkson {Decision::get_no_dimensions(regions.len())} else {0};

    let mut input_monitor = MonitorNOOP::new(1000); //MonitorNOOP::new(0); //MonitorMovingAverage::new(1000);
    let input_log_interval = 1;
    //let mut output_monitor = MonitorMovingAverage::new(1000); //MonitorNOOP::new(0);

    let module = "";
    let fun_name = "redundancy_elimination";
    let code = include_str!("python_redundancy_bridge.py");
    let fun = Python::with_gil(|py| {

        // Load python code as module
        let module = PyModule::from_code(py, code, "", module).unwrap();
        // Load arguments via python function "load_args"
        let kwargs = PyDict::new(py);
        kwargs.set_item("dsize", batch_size).unwrap();
        kwargs.set_item("use_clarkson", use_clarkson).unwrap();
        if let Some(optimizer) = optimizer {
            kwargs.set_item("optimizer", optimizer).unwrap();
        }
        let res = module.call_method("load_args", (), Some(kwargs));
        if let Err(e) = res {
            println!("Error in load_args: {}", e);
        }

        // Get reference to python function for redundancy elimination
        let fun: Py<PyAny> =
            //PyModule::import(py, module)
            module.getattr(fun_name)
            .unwrap()
            .into();
        fun
    });
    let fun: &Py<PyAny> = &*Box::leak(Box::new(fun));

    type Input = Decision;
    let mut batcher = BatcherMap::<Input>::new(batch_size);

    let mut reduce_input_monitor = MonitorNOOP::new(1000); //MonitorMovingAverage::new(1000);
    let mut reduce_batch_monitor = MonitorNOOP::new(1000); //MonitorMovingAverage::new(1000);
    let batch_logging_frequency = Some(1);
    let mut reduce_output_monitor =  MonitorMovingAverage::new(1000); // MonitorNOOP::new(0);
    let optimal_log_interval = Some(1000);

    let candidate_proto_sink = ProtobufFileSink::new(Path::new(&output_candidates_file_name), 1*1024*1024, 1024).unwrap();
    let optimal_proto_sink = ProtobufFileSink::new(Path::new(&output_file_name), 1*1024*1024, 1024).unwrap();

    let flow = hydroflow_syntax! {
        source_in = source_stream(input) -> map(|x| -> Vec<u16> {deserialize_from_bytes(x.unwrap()).unwrap()})
        -> inspect(|_|{
            input_monitor.add_arrival_time_now();
            input_monitor.print("Input:", Some(input_log_interval));
        }) -> tee();

        time_sink = union() -> dest_sink(time_sink);
        count_sink = dest_sink(count_sink);
        
        // Measure the total cycle time here
        tick_duration =
            source_in -> reduce(|_, _|()) // Current tick
            -> map(|_| context.current_tick_start()) // Tick of current tick
            -> defer_tick() // Wait for next tick
            // Duration between start of current tick and start of next tick
            -> map(|prev_tick| {context.current_tick_start() - prev_tick})
            -> map(|d|(SkyPieLogEntryType::Total, d));

        tick_duration -> map(|d|{serialize_to_bytes(d)}) -> time_sink;

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
        tombstone_signal = source_in -> demux(|v: Vec<u16>, var_args!(payload, tombstone)| {
            if v.len() == 0 {
                tombstone.give(v.clone());
            }
            payload.give(v);
        });

        // Signal termination of this worker after draining all work
        tombstone_signal[tombstone] -> defer_tick() -> map(|_|{serialize_to_bytes(worker_id)}) -> dest_sink(done_sink);

        candidates = tombstone_signal[payload] -> map(|write_choice_ids: Vec<u16>| {
            if write_choice_ids.len() > 0 {

                //object_store_id_map
                let object_stores = write_choice_ids.iter().map(|id|{
                    object_store_id_map.get(id).unwrap().clone()
                }).collect_vec();
                let write_choice = WriteChoice{object_stores};

                let write_choice = Box::<WriteChoice>::new(write_choice);
                //let mut i = 0; // for debugging
                let assignments = regions.iter()
                    .map(|r|{
                        // Get optimal assignments of region r
                        let assignments = opt_assignments(write_choice.clone(), r, &compatibility_checker_slos);
                        // Convert to (upper bound, object store) pairs 
                        let assignments = assignments.map(|(o, range)|(range.max, o));

                        // Collect into vector
                        return (r.clone(), assignments.collect::<Vec<(f64, ObjectStore)>>());
                    })
                    .filter(|(_, v)|{v.len() > 0}); // Filter out empty assignments

                // Merge assignments per region to candidate policies
                // XXX: Materializing here is not necessary
                // In HyrdoFlow, use stream of (region, upper bound, object store), then search min per region and then merge?
                let assignments = HashMap::from_iter(assignments);

                #[cfg(dev)]
                println!("Assignments: {}", assignments.len());

                // Each app region must have at least one assignment otherwise there is no feasible candidate placement
                if assignments.len() == regions.len() {
                    return MergeIterator::new(write_choice, assignments);
                } else {
                    // Print number of assignments per application region
                    
                    #[cfg(dev)]
                    for (r, v) in assignments.iter() {
                        println!("Region {}: {}", r.get_id(), v.len());
                    }

                    return MergeIterator::empty();
                }
            } else {
                return MergeIterator::tombstone();
            }
        }) -> flatten() -> tee();

        // Output candidates
        candidates
        -> filter(|d|{!d.is_tombstone()}) // Filter out tombstones for file output
        -> map(|d: Decision| -> skypie_proto_messages::Decision {d.into()})
        -> dest_sink(candidate_proto_sink);

        // XXX: Materializing decisions here, since putting lifetime into the Batcher is difficult
        reduce_input = candidates -> map(|x: Decision| x);

        batches = reduce_input -> inspect(|_|{
            reduce_input_monitor.add_arrival_time_now();
            reduce_input_monitor.print("Candidates in:", Some(1000));
        })
        // Collect batch of decisions, batcher returns either None or Some(batch)
        // Filter_map drops None values
        // XXX: Use hydro's batch operator. But it has to have sufficient items, about batch size!
        -> filter_map(|x: Input|{
            if x.is_tombstone() {
                println!("Flushing batcher: {}", batcher.get_batch().len());
                batcher.flush()
            } else {
                batcher.add(x)
            }
        })
        -> inspect(|_|{
            reduce_batch_monitor.add_arrival_time_now();
            reduce_batch_monitor.print("Batches:", batch_logging_frequency);
        });

        // Redundancy elimination via python
        optimal_zip = batches -> map(|decisions: Vec<Decision>| {
            let no_candidates = decisions.len();

            // Start time of computing optimal decisions
            let start = std::time::Instant::now();

            // Clarkson's algorithm cannot handle the case of fewer decisions than dimensions
            let optimal = if decisions.len() < no_dimensions {
                decisions
            }
            else {

                // Convert batch of decisions to numpy array
                let py_array = Decision::to_inequalities_numpy(&decisions);
    
                let optimal = Python::with_gil(|py| {
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
                });

                optimal
            };

            // End time of computing optimal decisions
            let end = std::time::Instant::now();
            let duration = end - start;

            let no_optimal = optimal.len();
            println!("Optimal: {}/{} (-{})", no_optimal, no_candidates,  no_candidates - no_optimal);
            /* if diff <= 5 {
                println!("Optimal: {}/{} (-{})", no_optimal, no_candidates,  no_candidates - no_optimal);
            } */

            (duration, optimal)
        })
        -> unzip();

        optimal_duration = optimal_zip[0];

        // Log number of optimal decisions with demux
        optimal = optimal_zip[1] -> demux(|v: Vec<Decision>, var_args!(payload, count)| {
            count.give(v.len());
            payload.give(v);
        });

        optimal[count] -> reduce(|acc: &mut usize, d|{*acc = *acc + d}) -> map(|d|(SkyPieLogEntryType::OptimalCount, d)) -> map(|d|{serialize_to_bytes(d)}) -> count_sink;

        // Output optimal
        optimal[payload] -> flatten()
            -> inspect(|_|{
                reduce_output_monitor.add_arrival_time_now();
                reduce_output_monitor.print("Optimal:", optimal_log_interval);
            })
            -> map(|d: Decision| -> skypie_proto_messages::Decision {d.into()})
            -> dest_sink(optimal_proto_sink);

        // Time of optimal
        optimal_duration
            // Total time of computing optimal in this tick
            -> reduce(|acc: &mut std::time::Duration, d|{*acc = *acc + d})
            -> map(|d|(SkyPieLogEntryType::RedundancyElimination, d))
            -> map(|d|{serialize_to_bytes(d)}) -> time_sink;

    };

    return flow;

}
