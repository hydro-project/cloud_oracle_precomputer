use clap::Parser;
use hydroflow::util::cli::{ConnectedDirect, ConnectedSource};
use hydroflow::util::deserialize_from_bytes;
use hydroflow::hydroflow_syntax;
use pyo3::types::PyModule;
use pyo3::{Python, PyAny, Py};
use skypie_lib::Decision;
use skypie_lib::skypie_lib::monitor::MonitorMovingAverage;
use skypie_lib::skypie_lib::reduce_oracle_hydroflow::BatcherMap;
use skypie_lib::Args;

#[hydroflow::main]
async fn main() {
    let mut ports = hydroflow::util::cli::init().await;

    let args = Args::parse();

    let input_recv = ports
        .port("input")
        // connect to the port with a single recipient
        .connect::<ConnectedDirect>() 
        .await
        .into_source();

    let module = "";
    let fun_name = "redundancy_elimination";
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

    type Input = skypie_lib::skypie_lib::candidate_policies_hydroflow::OutputType;
    let mut batcher = BatcherMap::<Input>::new(args.batch_size);

    let mut input_monitor = MonitorMovingAverage::new(1000);
    let mut batch_monitor = MonitorMovingAverage::new(1000);

    hydroflow::util::cli::launch_flow(hydroflow_syntax! {

        input = source_stream(input_recv) -> map(|x| -> Input {deserialize_from_bytes(x.unwrap()).unwrap()});
        batches = input -> inspect(|_|{
            input_monitor.add_arrival_time_now();
            input_monitor.print("Candidates in:", Some(1000));
            //println!("Decisions: {}", output_monitor);
            /* if output_monitor.get_count() % 1 == 0 {
                println!("{:?} outputs, at rate {:?}", output_monitor.get_count(), output_monitor.get_arrival_time_average().unwrap());
            } */
        })
        // Collect batch of decisions, batcher returns either None or Some(batch)
        // Filter_map drops None values
        // XXX: Use hydro's batch operator. But it has to have sufficient items, about batch size!
        -> filter_map(|x: Decision|{
            return batcher.add(x)
        })
        -> inspect(|_|{
            batch_monitor.add_arrival_time_now();
            batch_monitor.print("Batches:", None);
        });

        // Redundancy elimination via python
        /* batches -> map(|x: Vec<Input>|{
            //println!("Batch size: {}", x.len());
            x
        }) */
        batches
        -> map(|decisions| -> Vec<Decision> {
            // Convert batch of decisions to numpy array
            let py_array = Decision::to_inequalities_numpy(&decisions);

            Python::with_gil(|py| {
                type T = Vec<usize>;
    
                let py_res = fun.call(py, (py_array,), None).unwrap();
                // Computing optimal decisions by row IDs in vector
                let res: T = py_res.extract(py).unwrap();
    
                // Extract optimal decisions by ids in res
                let mut optimal = Vec::with_capacity(res.len());
                for id in res {
                    optimal.push(decisions[id].clone());
                }

                optimal
            })
        })
        //-> py_run(args.code, args.module, args.function)
        -> for_each(|x|{println!("{:?}", x);});
        /* -> for_each(|_: Vec<Decision>|{
            // Count decisions
        }); */
    }).await;
}