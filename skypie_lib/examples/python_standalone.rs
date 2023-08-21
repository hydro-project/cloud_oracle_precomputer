/* use hydroflow::util::cli::{ConnectedDirect, ConnectedSink, ConnectedSource};
use hydroflow::util::{deserialize_from_bytes, serialize_to_bytes}; */
use hydroflow::hydroflow_syntax;
use pyo3::types::PyModule;
use pyo3::{Python, Py, PyAny};
use numpy::ndarray::{Dim};
use numpy::{PyArray};

fn _version1(coefficients: Vec<Vec<f64>>, code: &str, module: &str, fun_name: &str) {

    // Load python with redundancy elimination
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

    /* 
    inequalities = np.array([
        [0] + [ c*-1 for c in coefficients_i ] + [1] for coefficients_i in coefficients
    ])
     */

    let inequalities: Vec<f64> = coefficients.iter().fold(Vec::<f64>::new(), |acc, e| {
        let mut acc = acc;
        acc.push(0.0);
        for c in e {
            acc.push(c * -1.0);
        }
        acc.push(1.0);
        acc
    });

    let dims = [coefficients.len(), coefficients[0].len() + 2];

    // Convert to numpy array
    let ineq_py: Py<PyArray<f64, Dim<[usize; 2]>>> = Python::with_gil(|py| {
        
        let py_array = PyArray::from_vec(py, inequalities);
        let shaped = py_array.reshape(dims).unwrap();
        
        let shape = shaped.shape();
        assert_eq!(dims, shape);

        let pypy_array = shaped.to_owned();

        pypy_array
    });

    let args = vec![ineq_py];

    type T = Vec<usize>;

    let mut flow = hydroflow_syntax! {
        
        source_iter(args) -> map(|x| -> T {
            Python::with_gil(|py| {
                let py_res = fun.call(py, (x,), None).unwrap();
                let res: T = py_res.extract(py).unwrap();

                res
            })
        }) -> for_each(|x| println!("{:?}", x));
    };

    //hydroflow::util::cli::launch_flow(flow).await;
    flow.run_available();
}

fn version2(coefficients: Vec<Vec<f64>>, code: &str, module: &str, fun_name: &str) {

    // Load python with redundancy elimination
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

    let args = vec![coefficients];

    type T = Vec<usize>;

    let mut flow = hydroflow_syntax! {
        
        source_iter(args) -> map(|coefficients: Vec<Vec<f64>>| -> T {
            // Convert coefficients to planes
            let num = coefficients.len();
            let dim = coefficients[0].len() + 2; // Include intercept and additional dimension for inequality
            let dims = [num, dim];

            // allocate 1-d vector for inequalities
            let mut ineqs: Vec<f64> = Vec::with_capacity(num * dim);
            for ineq in coefficients {
                ineqs.push(0.0); // Intercept
                for c in ineq {
                    ineqs.push(c * -1.0); // Coefficients for cost per workload feature of decision converted to negative
                }
                ineqs.push(1.0); // Coefficient of inequality, i.e., cost
            }
            
            Python::with_gil(|py| {
                // Push into numpy array
                let py_array = PyArray::from_vec(py, ineqs);
                // Reshape to 2-d array
                let shaped = py_array.reshape(dims).unwrap();

                let py_res = fun.call(py, (shaped,), None).unwrap();
                let res: T = py_res.extract(py).unwrap();

                res
            })
        }) -> for_each(|x| println!("{:?}", x));
    };

    //hydroflow::util::cli::launch_flow(flow).await;
    flow.run_available();
}

#[hydroflow::main]
async fn main() {

    let module = "";
    let fun_name = "redundancy_elimination";
    // Read python code from file at compile time in current directory
    let code = include_str!("../src/skypie_lib/python_redundancy_bridge.py");

    let coefficients: Vec<Vec<f64>> = vec![
        vec![0.5, 3.0], // f_1(x,y) = .5x + 3y
        vec![1.5, 1.5], // f_2(x,y) = 1.5x + 1.5y
        vec![2.5, 2.5] // f_2(x,y) = 1.5x + 1.5y
    ];

    version2(coefficients, code, module, fun_name);

}