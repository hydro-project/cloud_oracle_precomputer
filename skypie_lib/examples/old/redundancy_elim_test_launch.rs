use clap::Parser;
use hydroflow::util::cli::{ConnectedDirect, ConnectedSource};
use hydroflow::util::deserialize_from_bytes;
use hydroflow::hydroflow_syntax;
use pyo3::types::PyModule;
use pyo3::{Python, PyAny, Py};
use skypie_lib::Decision;
use skypie_lib::skypie_lib::decision::DecisionsExtractor;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;
use skypie_lib::skypie_lib::reduce_oracle_hydroflow::BatcherMap;
use skypie_lib::Args;

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;

    
    let input_recv = ports
    .port("input")
    // connect to the port with a single recipient
    .connect::<ConnectedDirect>() 
    .await
    .into_source();

    let args = Args::parse();

    let module = "";
    let fun_name = "redundancy_elimination";
    //let fun_name = "redundancy_elimination_dummy";
    // Read python code from file at compile time in current directory
    let code = include_str!("../src/skypie_lib/python_redundancy_bridge.py");
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

    type Input = skypie_lib::skypie_lib::candidate_policies_hydroflow::OutputType;
    let mut batcher = BatcherMap::<Input>::new(args.batch_size);

    let mut reduce_input_monitor = MonitorMovingAverage::new(1000);
    let mut reduce_batch_monitor = MonitorMovingAverage::new(1000);
    let mut reduce_output_monitor = MonitorMovingAverage::new(1000);

    let flow = hydroflow_syntax! {

        input = source_stream(input_recv) -> map(|x| -> Input {deserialize_from_bytes(x.unwrap()).unwrap()});
        batches = input -> inspect(|_|{
            reduce_input_monitor.add_arrival_time_now();
            reduce_input_monitor.print("Candidates in:", Some(1000));
        })
        // Collect batch of decisions, batcher returns either None or Some(batch)
        // Filter_map drops None values
        // XXX: Use hydro's batch operator. But it has to have sufficient items, about batch size!
        -> filter_map(|x: Decision|{
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
            reduce_output_monitor.print("Optimal:", Some(1));
        });
    };

    println!("Launching");
    hydroflow::util::cli::launch_flow(flow).await;
}