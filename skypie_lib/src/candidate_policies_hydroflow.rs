use hydroflow::{hydroflow_syntax, tokio_stream::Stream, bytes::{BytesMut, Bytes}, util::{deserialize_from_bytes, serialize_to_bytes}, futures::Sink};

use crate::{write_choice::WriteChoice, opt_assignments::opt_assignments, merge_policies::{AssignmentsRef, MergeIteratorRef, ApplicationRegion}, decision::DecisionRef, Decision, object_store::ObjectStore, range::Range};

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
        });

        // Get optimal assignments of region r
        assignments = source_in -> flat_map(|(w, r): (Box::<WriteChoice>, &'static ApplicationRegion) | {
            opt_assignments(w.clone(), r).map(move |x| {(w.clone(), (r,x))} )
        });

        // Convert to (upper bound, object store) pairs 
        converted_assignments = assignments -> map(|(w, (r, (o, range))): (Box<WriteChoice>, (&'static ApplicationRegion, (ObjectStore, Range)))| { (w, (r, (range.max, o)))});

        assignments_acc = converted_assignments -> fold_keyed(||{AssignmentsRef::new()}, |acc: &mut AssignmentsRef, (r, val)|{
            let v = acc.entry(r).or_insert(vec![]);
            v.push(val);
        });

        // Create merge iterator
        candidates = assignments_acc -> flat_map(|(write_choice, assignments):(Box<WriteChoice>, _)| {
            //MergeIterator::new(write_choice, assignments)
            MergeIteratorRef::new(write_choice, assignments)
        }) -> inspect(|_|{
            output_monitor.add_arrival_time_now();
            output_monitor.print("Candidates out: ", Some(1000));
        });

        // Push into reduce_oracle
        candidates -> map(|x: DecisionRef|{
            //x.write_choice
            
            let b = serialize_to_bytes(x.clone());
            //let des = deserialize_from_bytes::<Decision>(b.as_ref()).unwrap();
            //let eq = x == des;
            debug_assert_eq!(x, deserialize_from_bytes::<Decision>(b.as_ref()).unwrap());
            b
        })
        -> dest_sink(output);

    };

    return flow;

}