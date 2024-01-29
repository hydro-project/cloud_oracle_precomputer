use pyo3::{Python, types::{PyModule, PyDict}};

// Get the optimizer json from the python bridge, as tuple of optimizer name and json string
pub fn get_optimizer_json(batch_size: usize, optimizer: Option<String>, use_clarkson: bool) -> (String, String)
{
    let module = "";
    let code = include_str!("python_redundancy_bridge.py");

    Python::with_gil(|py| {

        let module =
            PyModule::from_code(py, code, "", module).unwrap();

        // Load arguments
        let kwargs = PyDict::new(py);
        kwargs.set_item("dsize", batch_size).unwrap();
        kwargs.set_item("use_clarkson", use_clarkson).unwrap();
        if let Some(optimizer) = optimizer {
            kwargs.set_item("optimizer", optimizer).unwrap();
        }
        
        module.call_method("load_args", (), Some(kwargs)).unwrap();
        // Get arguments as json
        let res: (String, String) = module.call_method0("get_optimizer_json") .unwrap().extract().unwrap();

        return res;
    })
}