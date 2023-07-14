use itertools::Itertools;
use numpy::ndarray::Dim;
use numpy::PyArray;
use pyo3::{Py, Python};

use crate::skypie_lib::read_choice::ReadChoice;
use crate::skypie_lib::write_choice::WriteChoice;
use crate::{ApplicationRegion};

use super::object_store::ObjectStore;
use super::output::{OutputDecision, OutputScheme, OutputAssignment};
use super::read_choice::{ReadChoiceRef, ReadChoiceIter, ReadChoiceRefIter};

#[derive(Clone, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub struct Decision {
    // Write Choice
    pub write_choice: WriteChoice,
    // Read Choice
    // TODO: Double check that read choices have identical order for all decisions!
    pub read_choice: ReadChoice,
}

impl Decision {
    pub fn cost_iter<'a>(&'a self) -> DecisionCostIter<'a> {
        DecisionCostIter::new(self)
    }
}

pub struct DecisionCostIter<'a> {
    decision: &'a Decision,
    get_iter: ReadChoiceIter::<'a>, // hash_map::Iter<'a, ApplicationRegion, ObjectStore>,
    ingress_iter: ReadChoiceIter::<'a>, //hash_map::Iter<'a, ApplicationRegion, ObjectStore>,
    egress_iter: ReadChoiceIter::<'a>, //hash_map::Iter<'a, ApplicationRegion, ObjectStore>,
    num_apps: usize,
    pos: usize,
}

impl<'a> DecisionCostIter<'a> {
    pub fn new(decision: &'a Decision) -> DecisionCostIter<'a> {
        let num_apps = decision.read_choice.len();
        let assignments = decision.read_choice.iter(); //: hash_map::Iter<'_, ApplicationRegion, ObjectStore> = decision.read_choice.iter();
        DecisionCostIter {
            decision,
            get_iter: assignments.clone(),
            ingress_iter: assignments.clone(),
            egress_iter: assignments.clone(),
            num_apps,
            pos: 0,
        }
    }

    pub fn len(&self) -> usize {
        /* storage, put, get..., ingress..., egress... */
        1 + 1 + self.num_apps + self.num_apps + self.num_apps
    }
}

impl Iterator for DecisionCostIter<'_> {
    type Item = f64;

    /*  Return cost of decision element by element.
        Layout is of cost, for n = |apps|
        storage
        put
        get_0 ... get_n
        ingress_0 ... ingress_n
        egress_0 ... egress_n

    */
    fn next(&mut self) -> Option<Self::Item> {

        let num_apps = self.num_apps;

        let get_start = 2;
        let get_end = 2 + num_apps;
        let ingress_start = get_end;
        let ingress_end = ingress_start + num_apps;
        let egress_start = ingress_end;
        let egress_end = egress_start + num_apps;

        let pos = self.pos;
        let res = if pos == 0 {
            // Storage: sum of object stores' storage costs
            let cost = self
                .decision
                .write_choice
                .object_stores
                .iter()
                .fold(0.0, |acc, x: &ObjectStore| acc + x.cost.size_cost);
            Some(cost)
        } else if pos == 1 {
            // Put: sum of object stores' put costs
            let cost = self
                .decision
                .write_choice
                .object_stores
                .iter()
                .fold(0.0, |acc, x: &ObjectStore| acc + x.cost.put_cost);
            Some(cost)
        } else if pos >= get_start && pos < get_end {
            // Get costs of object store assigned to application region
            Some(self.get_iter.next().unwrap().1.cost.get_cost)
        } else if pos >= ingress_start && pos < ingress_end {
            // Ingress is the sum of a particular app region's egress cost and the ingress costs of an object store, for all object stores
            let app_region: &ApplicationRegion = &self.ingress_iter.next().unwrap().0;
            let cost = self
                .decision
                .write_choice
                .object_stores
                .iter()
                .fold(0.0, |acc, o: &ObjectStore| {
                    acc + o.get_ingress_cost(&app_region)
                });
            Some(cost)
        } else if pos >= egress_start && pos < egress_end {
            // Egress is ingress cost of a particular app region and the egress cost of it's assigned object store
            let (app_region, object_store) =
                &self.egress_iter.next().unwrap();
            let cost = object_store.get_egress_cost(&app_region);
            Some(cost)
        } else {
            None
        };

        self.pos += 1;

        res
    }
}

impl Decision {
    pub fn to_inequalities_numpy(decisions: &Vec<Decision>) -> Py<PyArray<f64, Dim<[usize; 2]>>> {
        let dim = 2 + decisions.first().unwrap().cost_iter().len();
        let num = decisions.len();
        let dims = [num, dim];

        // allocate 1-d vector for inequalities
        let mut ineqs: Vec<f64> = Vec::with_capacity(num * dim);
        for decision in decisions {
            // Intercept
            ineqs.push(0.0);
            // Coefficients for cost per workload feature of decision converted to negative
            //ineqs.extend(decision.cost_iter().map(|c| c * -1.0));
            ineqs.extend(decision.cost_iter().map(|c| c * 1.0));
            // Coefficient of inequality, i.e., cost
            ineqs.push(-1.0);
        }

        debug_assert_eq!(ineqs.len(), num * dim);

        Python::with_gil(|py| {
            // Push into numpy array
            let py_array = PyArray::from_vec(py, ineqs);
            // Reshape to 2-d array
            let shaped = py_array.reshape(dims).unwrap();

            debug_assert_eq!(shaped.shape(), dims);

            shaped.to_owned()
        })
    }
}

impl From<Decision> for OutputDecision {
    fn from(decision_ref: Decision) -> Self {
        let mut cost_wl_halfplane: Vec::<f64> = Vec::with_capacity(2 + decision_ref.cost_iter().len());
        
        cost_wl_halfplane.push(0.0);
        // Coefficients for cost per workload feature of decision converted to negative
        //ineqs.extend(decision.cost_iter().map(|c| c * -1.0));
        cost_wl_halfplane.extend(decision_ref.cost_iter().map(|c| c * 1.0));
        // Coefficient of inequality, i.e., cost
        cost_wl_halfplane.push(-1.0);
        
        let replication_scheme: OutputScheme = decision_ref.into();
        
        //let cost_wl_halfplane = ;
        OutputDecision { replication_scheme, cost_wl_halfplane }
    }
}

impl From<Decision> for OutputScheme {
    fn from(decision: Decision) -> Self {
        let object_stores = decision.write_choice.object_stores.into_iter().map(|o| format!("{}-{}", o.region.name, o.name)).collect_vec();
        let app_assignments = decision.read_choice.iter().map(|(region, object_store)| OutputAssignment{app: region.region.name.clone(), object_store: format!("{}-{}", object_store.region.name, object_store.name)}).collect_vec();
        OutputScheme{object_stores, app_assignments}
    }
}


impl Default for Decision {
    fn default() -> Self {
        Decision {
            write_choice: WriteChoice::default(),
            read_choice: ReadChoice::default(),
        }
    }
}

#[derive(Clone, PartialEq, Debug, serde::Serialize)]
pub struct DecisionRef<'a> {
    // Write Choice
    pub write_choice: Box<WriteChoice>,
    // Read Choice
    pub read_choice: ReadChoiceRef<'a>,
}

impl<'b> DecisionRef<'b> {
    pub fn cost_iter<'a>(&'a self) -> DecisionCostIterRef<'b,'a> {
        DecisionCostIterRef::new(self)
    }
}

pub struct DecisionCostIterRef<'b, 'a> {
    decision: &'a DecisionRef<'b>,
    get_iter: ReadChoiceRefIter::<'b, 'a>, // hash_map::Iter<'a, ApplicationRegion, ObjectStore>,
    ingress_iter: ReadChoiceRefIter::<'b, 'a>, //hash_map::Iter<'a, ApplicationRegion, ObjectStore>,
    egress_iter: ReadChoiceRefIter::<'b, 'a>, //hash_map::Iter<'a, ApplicationRegion, ObjectStore>,
    num_apps: usize,
    pos: usize,
}

impl<'b, 'a> DecisionCostIterRef<'b, 'a> {
    pub fn new(decision: &'a DecisionRef<'b>) -> DecisionCostIterRef<'b, 'a> {
        let num_apps = decision.read_choice.len();
        let assignments = decision.read_choice.iter(); //: hash_map::Iter<'_, ApplicationRegion, ObjectStore> = decision.read_choice.iter();
        DecisionCostIterRef {
            decision,
            get_iter: assignments.clone(),
            ingress_iter: assignments.clone(),
            egress_iter: assignments.clone(),
            num_apps,
            pos: 0,
        }
    }

    pub fn len(&self) -> usize {
        /* storage, put, get..., ingress..., egress... */
        1 + 1 + self.num_apps + self.num_apps + self.num_apps
    }
}

impl Iterator for DecisionCostIterRef<'_,'_> {
    type Item = f64;

    /*  Return cost of decision element by element.
        Layout is of cost, for n = |apps|
        storage
        put
        get_0 ... get_n
        ingress_0 ... ingress_n
        egress_0 ... egress_n

    */
    fn next(&mut self) -> Option<Self::Item> {

        let num_apps = self.num_apps;

        let get_start = 2;
        let get_end = 2 + num_apps;
        let ingress_start = get_end;
        let ingress_end = ingress_start + num_apps;
        let egress_start = ingress_end;
        let egress_end = egress_start + num_apps;

        let pos = self.pos;
        let res = if pos == 0 {
            // Storage: sum of object stores' storage costs
            let cost = self
                .decision
                .write_choice
                .object_stores
                .iter()
                .fold(0.0, |acc, x: &ObjectStore| acc + x.cost.size_cost);
            Some(cost)
        } else if pos == 1 {
            // Put: sum of object stores' put costs
            let cost = self
                .decision
                .write_choice
                .object_stores
                .iter()
                .fold(0.0, |acc, x: &ObjectStore| acc + x.cost.put_cost);
            Some(cost)
        } else if pos >= get_start && pos < get_end {
            // Get costs of object store assigned to application region
            Some(self.get_iter.next().unwrap().1.cost.get_cost)
        } else if pos >= ingress_start && pos < ingress_end {
            // Ingress is the sum of a particular app region's egress cost and the ingress costs of an object store, for all object stores
            let app_region: &ApplicationRegion = &self.ingress_iter.next().unwrap().0;
            let cost = self
                .decision
                .write_choice
                .object_stores
                .iter()
                .fold(0.0, |acc, o: &ObjectStore| {
                    acc + o.get_ingress_cost(&app_region)
                });
            Some(cost)
        } else if pos >= egress_start && pos < egress_end {
            // Egress is ingress cost of a particular app region and the egress cost of it's assigned object store
            let (app_region, object_store) =
                &self.egress_iter.next().unwrap();
            let cost = object_store.get_egress_cost(&app_region);
            Some(cost)
        } else {
            None
        };

        self.pos += 1;

        res
    }
}

// Convert from DecisionRef to Decision
impl From<DecisionRef<'_>> for Decision {
    fn from(decision_ref: DecisionRef<'_>) -> Self {
        let mut read_choice = ReadChoice::new(decision_ref.read_choice.len());
        for (region, object_store) in decision_ref.read_choice.iter() {
            read_choice.insert((*region).clone(), object_store.clone());
        }

        Decision {
            write_choice: *decision_ref.write_choice,
            read_choice: read_choice,
        }
    }
}

impl From<DecisionRef<'_>> for OutputDecision {
    fn from(decision_ref: DecisionRef<'_>) -> Self {
        let mut cost_wl_halfplane: Vec::<f64> = Vec::with_capacity(2 + decision_ref.cost_iter().len());
        
        cost_wl_halfplane.push(0.0);
        // Coefficients for cost per workload feature of decision converted to negative
        //ineqs.extend(decision.cost_iter().map(|c| c * -1.0));
        cost_wl_halfplane.extend(decision_ref.cost_iter().map(|c| c * 1.0));
        // Coefficient of inequality, i.e., cost
        cost_wl_halfplane.push(-1.0);
        
        let replication_scheme: OutputScheme = decision_ref.into();
        
        //let cost_wl_halfplane = ;
        OutputDecision { replication_scheme, cost_wl_halfplane }
    }
}

impl From<DecisionRef<'_>> for OutputScheme {
    fn from(decision_ref: DecisionRef<'_>) -> Self {
        let object_stores = decision_ref.write_choice.object_stores.into_iter().map(|o| format!("{}-{}", o.region.name, o.name)).collect_vec();
        let app_assignments = decision_ref.read_choice.iter().map(|(region, object_store)| OutputAssignment{app: region.region.name.clone(), object_store: format!("{}-{}", object_store.region.name, object_store.name)}).collect_vec();
        OutputScheme{object_stores, app_assignments}
    }
}

impl PartialEq<Decision> for DecisionRef<'_> {
    fn eq(&self, other: &Decision) -> bool {
        // Test if the hashmaps of the read_choices are equal
        let mut read_choice_equal = true;
        for (region, object_store) in self.read_choice.iter() {
            if !other.read_choice.contains_key(region) {
                read_choice_equal = false;
                break;
            }
            if other.read_choice.get(region) != *object_store {
                read_choice_equal = false;
                break;
            }
        }

        *self.write_choice == other.write_choice && read_choice_equal
    }
}

pub struct DecisionsExtractor {
    decisions: Vec<Decision>,
    index_iter: std::vec::IntoIter<usize>,
}

impl DecisionsExtractor {
    pub fn new(decisions: Vec<Decision>, indexes: Vec<usize>) -> Self {
        let index_iter: std::vec::IntoIter<usize> = indexes.into_iter();
        DecisionsExtractor {
            decisions,
            index_iter
        }
    }
}

impl Iterator for DecisionsExtractor {
    type Item = Decision;

    fn next(&mut self) -> Option<Self::Item> {
        self.index_iter.next().map(|i| self.decisions[i].clone())
    }
}

/*
let inequalities: Vec<f64> = coefficients.iter().fold(Vec::<f64>::new(), |acc, e| {
        let mut acc = acc;
        acc.push(0.0);
        for c in e {
            acc.push(c * -1.0);
        }
        acc.push(1.0);
        acc
    });
 */

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use hydroflow::hydroflow_syntax;
    use itertools::Itertools;
    use numpy::ndarray::Dim;
    use numpy::PyArray;
    use pyo3::types::PyModule;
    use pyo3::{Py, PyAny, Python};
    use test::Bencher;

    use crate::skypie_lib::object_store::{ObjectStore, Cost};
    use crate::skypie_lib::read_choice::ReadChoice;
    use crate::{Region, ApplicationRegion, Decision, WriteChoice};
    use crate::skypie_lib::network_record::NetworkCostMap;

    #[test]
    fn test_decision_cost() {
        assert!(false);

        /* let regions = vec![Region{id:0, name: "0".to_string()} ,Region{id: 0, name: "1".to_string()}];
        let egress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 1.0),
            (regions[1].clone(), 2.0)
        ]);
        let ingress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 0.0),
            (regions[1].clone(), 0.0)
        ]);

        let app_regions = regions.iter().map(|r|{ApplicationRegion{region: r.clone(), egress_cost: egress_cost.clone(), ingress_cost: ingress_cost.clone()}}).collect_vec();
        
        let mut object_stores = vec![
            ObjectStore{id: 0, name: "0".to_string(), region: regions[0].clone(), cost: Cost { size_cost: 1.0, put_cost: 2.0, put_transfer: 4.0, get_cost: 3.0, get_transfer: 5.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }},
            ObjectStore{id: 1, name: "1".to_string(), region: regions[1].clone(), cost: Cost { size_cost: 10.0, put_cost: 20.0, put_transfer: 10.0, get_cost: 30.0, get_transfer: 20.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }}
        ];

        for  o in object_stores.iter_mut() {
            o.cost.add_egress_costs(egress_cost.clone());
            o.cost.add_ingress_costs(ingress_cost.clone());
        }

        let o0 = &object_stores[0];
        let o1 = &object_stores[1];
        let a0 = &app_regions[0];
        // Egress of o0 to a0 including get_transfer + Ingress of a0 from o0: 1.0 + 5.0 + 0.0
        assert_eq!(o0.get_egress_cost(a0), 6.0 );
        // Ingress of o0 from a0 including put_transfer + Egress of a0 from o0: 0.0 + 4.0 + 1.0
        assert_eq!(o0.get_ingress_cost(a0), 5.0 );

        let decisions = vec![
            Decision{ write_choice: WriteChoice{object_stores: vec![o0.clone()]},
                read_choice: ReadChoice::from_iter(vec![(app_regions[0].clone(), o0.clone())])},
            Decision{ write_choice: WriteChoice{object_stores: object_stores.clone()},
                read_choice: ReadChoice::from_iter(vec![(app_regions[0].clone(), o0.clone()), (app_regions[1].clone(), o1.clone())])}
        ];

        // Test if cost of decision is the expected cost of the object store
        let d_cost = decisions[0].cost_iter().collect_vec();
        assert_eq!(d_cost[0], o0.cost.size_cost);
        assert_eq!(d_cost[1], o0.cost.put_cost);
        assert_eq!(d_cost[2], o0.cost.get_cost);
        assert_eq!(d_cost[3], o0.get_ingress_cost(&app_regions[0]));
        assert_eq!(d_cost[4], o0.get_egress_cost(&app_regions[0]));
        assert_eq!(d_cost.len(), 5);

        let d_cost = decisions[1].cost_iter().collect_vec();
        assert_eq!(d_cost[0], o0.cost.size_cost + o1.cost.size_cost);
        assert_eq!(d_cost[1], o0.cost.put_cost + o1.cost.put_cost);
        assert_eq!(d_cost[2], o0.cost.get_cost);
        assert_eq!(d_cost[3], o1.cost.get_cost);
        assert_eq!(d_cost[4], o0.get_ingress_cost(&app_regions[0]));
        assert_eq!(d_cost[5], o1.get_ingress_cost(&app_regions[1]));
        assert_eq!(d_cost[6], o0.get_egress_cost(&app_regions[0]));
        assert_eq!(d_cost[7], o1.get_egress_cost(&app_regions[1]));
        assert_eq!(d_cost.len(), 8); */

    }

    #[test]
    fn test_decision_to_inequalities() {/* 
        assert!(false);

        let regions = vec![Region{id:0, name: "0".to_string()} ,Region{id: 0, name: "1".to_string()}];
        let egress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 1.0),
            (regions[1].clone(), 2.0)
        ]);
        let ingress_cost = NetworkCostMap::from_iter(vec![
            (regions[0].clone(), 0.0),
            (regions[1].clone(), 0.0)
        ]);

        let app_regions = regions.iter().map(|r|{ApplicationRegion{region: r.clone(), egress_cost: egress_cost.clone(), ingress_cost: ingress_cost.clone()}}).collect_vec();
        
        let mut object_stores = vec![
            ObjectStore{id: 0, name: "0".to_string(), region: regions[0].clone(), cost: Cost { size_cost: 1.0, put_cost: 2.0, put_transfer: 4.0, get_cost: 3.0, get_transfer: 5.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }},
            ObjectStore{id: 1, name: "1".to_string(), region: regions[1].clone(), cost: Cost { size_cost: 10.0, put_cost: 20.0, put_transfer: 10.0, get_cost: 30.0, get_transfer: 20.0, egress_cost: HashMap::default(), ingress_cost: HashMap::default() }}
        ];

        for  o in object_stores.iter_mut() {
            o.cost.add_egress_costs(egress_cost.clone());
            o.cost.add_ingress_costs(ingress_cost.clone());
        }

        let o0 = &object_stores[0];
        let o1 = &object_stores[1];
        let a0 = &app_regions[0];
        // Egress of o0 to a0 including get_transfer + Ingress of a0 from o0: 1.0 + 5.0 + 0.0
        assert_eq!(o0.get_egress_cost(a0), 6.0 );
        // Ingress of o0 from a0 including put_transfer + Egress of a0 from o0: 0.0 + 4.0 + 1.0
        assert_eq!(o0.get_ingress_cost(a0), 5.0 );

        let decisions = vec![
            Decision{ write_choice: WriteChoice{object_stores: object_stores.clone()},
                read_choice: ReadChoice::from_iter(vec![(app_regions[0].clone(), o0.clone()), (app_regions[1].clone(), o1.clone())])},
            Decision{ write_choice: WriteChoice{object_stores: object_stores.clone()},
                read_choice: ReadChoice::from_iter(vec![(app_regions[0].clone(), o1.clone()), (app_regions[1].clone(), o0.clone())])}
        ];

        let res = Decision::to_inequalities_numpy(&decisions);
        let dim_expected= 2 + 2 + 2 * 3;
        Python::with_gil(|py|{
            let shape: [usize; 2] = res.getattr(py, "shape").unwrap().extract(py).unwrap();
            assert_eq!(shape, [2, dim_expected]);
        });
 */
    }

    fn version1(coefficients: Vec<Vec<f64>>, code: &str, module: &str, fun_name: &str) {
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

        let mut flow = hydroflow_syntax! {

            source_iter(args) -> map(|x| {
                Python::with_gil(|py| {
                    let py_res = fun.call(py, (x,), None).unwrap();
                    //let res: T = py_res.extract(py).unwrap();

                    py_res
                })
            }) -> for_each(|x| println!("{:?}", x));
        };

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

        let mut flow = hydroflow_syntax! {

            source_iter(args) -> map(|coefficients: Vec<Vec<f64>>| {
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
                    py_res
                })
            }) -> for_each(|x| println!("{:?}", x));
        };

        //hydroflow::util::cli::launch_flow(flow).await;
        flow.run_available();
    }

    fn bench(version: usize) {
        let num = 200;
        let dim = 100;
        let coefficients = vec![vec![1.0; dim]; num];

        let code = r#"
def fn(ineqs):
    return ineqs
        "#;
        let fun_name = "fn";
        let module = "test";

        match version {
            1 => version1(coefficients, code, module, fun_name),
            2 => version2(coefficients, code, module, fun_name),
            _ => panic!("Invalid version"),
        }
    }

    #[bench]
    fn bench_decision_to_ineq_v1(b: &mut Bencher) {
        b.iter(|| {
            bench(1);
        });
    }

    #[bench]
    fn bench_decision_to_ineq_v2(b: &mut Bencher) {
        b.iter(|| {
            bench(1);
        });
    }
}
