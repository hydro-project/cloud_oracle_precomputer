use itertools::Itertools;
use ndarray::Array2;
use pyo3::prelude::*;
use pyo3::types::PyList;
use rayon::prelude::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use skypie_lib::identifier::Identifier;
use skypie_lib::write_choice;
use std::collections::HashMap;

use skypie_lib::{
    object_store::ObjectStore, read_choice::ReadChoice, ApplicationRegion,
    Decision, WriteChoice,
};

use super::optimizer::{Optimizer, OptimizerData};
use super::workload::Workload;

#[pyclass]
#[derive(Debug)]
pub struct KmeansOptimizer {
    object_stores: Vec<ObjectStore>,
    application_regions: Vec<ApplicationRegion>,
    num_replicas: usize,
    max_num_replicas: usize,
    s_capital: Vec<usize>, // List of object store indexes
    c_capital: Vec<usize>, // List of client indexes
    //AStranslate: HashMap<String, usize>, // Map of application region names to indexes
    //PriceGet: HashMap<String, f64>, // Map of object store names to get prices
    //PriceNet: HashMap<String, HashMap<String, f64>>, // Map of object store names to map of application region names to net prices
    //dest: HashMap<String, usize>,
    max_iterations: usize,
    threshold: f32,
}

#[pymethods]
impl KmeansOptimizer {
    #[new]
    #[pyo3(signature = (network_file, object_store_file, object_stores_considered, application_regions_considered, num_replicas, max_iterations = 100, verbose = 0, threshold = 0.1, max_num_replicas = None))]
    pub fn new(
        network_file: &str,
        object_store_file: &str,
        object_stores_considered: Vec<&str>,
        application_regions_considered: HashMap<&str, u16>,
        num_replicas: usize,
        max_iterations: usize,
        verbose: usize,
        threshold: f32,
        max_num_replicas: Option<usize>
    ) -> Self {
        let max_num_replicas = max_num_replicas.unwrap_or(num_replicas);
        if num_replicas == 0 {
            panic!("num_replicas must be greater than 0");
        }
        if num_replicas > max_num_replicas {
            panic!("num_replicas must be smaller than max_num_replicas");
        }

        let OptimizerData {
            object_stores,
            application_regions,
        } = Self::load(
            network_file,
            object_store_file,
            object_stores_considered,
            application_regions_considered,
        );

        let mut application_regions = application_regions;
        application_regions.sort_by_key(|a|a.get_id());
        let mut object_stores = object_stores;
        object_stores.sort_by_key(|o|o.get_id());
        let s_capital = Vec::from_iter(0..object_stores.len()); //[0..object_stores.len()];
        let c_capital = Vec::from_iter(0..application_regions.len());

        //let s_capital = application_regions.iter().enumerate().map(|(i, _)|i).collect_vec();
        //let c_capital = object_stores.iter().enumerate().map(|(i,_)|i).collect_vec();

        Self {
            object_stores,
            application_regions,
            num_replicas,
            max_num_replicas,
            s_capital,
            c_capital,
            max_iterations,
            threshold,
        }
    }

    pub fn optimize(&self, workload: &Workload) -> (f64, i32) {
        let (cost, decision) = self._optimize(workload);

        /* print!("Write choice:");
        for o in &decision.write_choice.object_stores {
            print!(" {}-{}", o.region.name, o.name);
        }
        println!();
        print!("Read choice: ");
        for (app, o) in decision.read_choice.iter() {
            print!(" {} -> {}-{}", app.region.name, o.region.name, o.name);
        }
        println!();

        io::stdout().flush().unwrap(); */


        (cost, decision.write_choice.object_stores.len() as i32)
    }

    pub fn optimize_batch<'py>(&self, workloads: &'py PyList) -> Vec<(f64, i32)> {
        
        let workloads = workloads.iter().map(|x|{
            let w = x.extract::<PyRef<'py,Workload>>().unwrap();
            (*w).clone()
        }).collect_vec();
        
        workloads.par_iter().map(|w| {self.optimize(&w)}).collect()
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl Optimizer for KmeansOptimizer {
    
    fn _optimize(&self, w: &Workload) -> (f64, Decision) {

        (self.num_replicas..=self.max_num_replicas).into_iter().map(|num_replicas| {

            // Compute read costs (get + egress) of each application for each object store under given workloads
            // Optimization to avoid recomputation
            let mut get_costs_o_c = Array2::<f32>::zeros((self.s_capital.len(), self.c_capital.len()));
            for o in &self.s_capital {
                for c in &self.c_capital {
                    get_costs_o_c[(*o, *c)] = self.object_stores[*o].compute_read_costs(
                        &self.application_regions[*c],
                        w.get_gets(*c),
                        w.get_egress(*c),
                    ) as f32;
                }
            }
    
            let decision = self.weighted_k_means(w, &get_costs_o_c, num_replicas);
    
            let cost = self.cost(w, &decision);
    
            // XXX: Implement serialization of decision, but we don't care right now.
            //(cost, decision)
            (cost, decision)
        }).reduce(|(cost, decision), (cost2, decision2)| {
            if cost < cost2 {
                (cost, decision)
            } else {
                (cost2, decision2)
            }
        }).expect("No kmeans solutions found!")

    }
}

impl KmeansOptimizer {
    
    fn weighted_k_means(&self, w: &Workload, get_costs_o_c: &Array2<f32>, num_replicas: usize) -> Decision {

        // IDs of application regions
        let c_capital = &self.c_capital; //.clone();
        let s_capital = &self.s_capital; //.clone();
        // TODO: Check if to include egress costs?
        let w_c = c_capital.iter().map(|&c| w.get_gets(c)).collect::<Vec<_>>();
        //4: // pick initial centroids
        //5: G ← Lfixed
        let mut g_capital = Vec::new();
        //6: sort all client clusters c ∈ C by descending wc
        let c_capital_sorted = c_capital.iter().sorted_by(|&a, &b| w_c[*b].partial_cmp(&w_c[*a]).unwrap()).collect_vec();
        //7: while |G| < num replicas and more client clusters remain
        //8: c ← next client cluster in C
        for &c in c_capital_sorted {
            if g_capital.len() >= num_replicas {
                break;
            }

            //9: if not nearest(c, S) ∈ G then
            //10: add nearest(c, S) to G
            let n = self.nearest(c, &s_capital, &get_costs_o_c);
            if !g_capital.contains(&n) {
                g_capital.push(n);
            }
        }

        #[cfg(dev)]
        println!("Initial centroids: {:?}", g_capital);

        //11: new cost ← cost(G)
        let mut new_cost = self.cost_read(&c_capital, &g_capital, &get_costs_o_c);
        //12: repeat
        let mut cg = HashMap::new();
        for i in 0..self.max_iterations {
            #[cfg(dev)] {
                println!("K-means iteration {}/{}", i+1, self.max_iterations);
            }
            //13: prev cost ← new cost
            let prev_cost = new_cost;
            //14: // cluster clients according to nearest centroid
            //15: ∀g ∈ G let Cg ← {c | g = nearest(c, G)}
            cg.clear();
            for &c in c_capital {
                let g = self.nearest(c, &g_capital, &get_costs_o_c);
                cg.entry(g).or_insert_with(Vec::new).push(c);
            }
            //16: // attempt to adjust centroids
            //17: for each g ∈ G \ Lfixed
            for g in &mut g_capital {
                
                // Equal access costs of two object stores lead to empty clusters!
                if !cg.contains_key(&g) {continue;}

                //18: g′ ← v ∈ S s.t. ∑ c∈Cg wc · rtt(i) c,v is minimized
                let g_new = self.argmin(&s_capital, &cg[&g], &get_costs_o_c);

                #[cfg(dev)]
                if *g != g_new {
                    println!("Centroid {} changed to {}", g, g_new);
                }

                //19: update centroid g to g′
                *g = g_new;
            }
            //20: new cost ← cost(G)
            new_cost = self.cost_read(&c_capital, &g_capital, &get_costs_o_c);
            //21: until new cost − prev cost < threshold
            if prev_cost - new_cost < self.threshold {
                #[cfg(dev)] {
                    println!("K-means converged after {} iterations at: {}", i+1, new_cost);
                }
                break;
            }

            #[cfg(dev)] {
                println!("K-means {}: {} -> {}", i, prev_cost, new_cost);
            }
        }
        //22: return G
        #[cfg(dev)]
        if g_capital.len() != num_replicas as usize {
            println!("Unexpected number of replicas: expected {}, got {}, Write Choice: {:?}, Read Choice: {:?}", num_replicas, g_capital.len(), g_capital, cg);
        }
        
        // Translate Cg to ReadChoice
        let mut read_choice = ReadChoice::new(self.application_regions.len());
        for (g, c) in cg {
            let o = self.object_stores[g].clone();
            for c in c {
                let app = self.application_regions[c].clone();
                read_choice.insert(app, o.clone());
            }
        }

        // Translate G to Write Choice
        let write_choice = WriteChoice{object_stores: g_capital.iter().map(|&g| self.object_stores[g].clone()).collect()};

        Decision {write_choice, read_choice}
    }
     
    fn cost_read(&self, clients: &[usize], object_stores: &[usize], get_costs_o_c: &Array2<f32>) -> f32 {
        let mut res = 0.0;
        for &o in object_stores {
            for &c in clients {
                res += get_costs_o_c[(o, c)];
            }
        }
        res
    }

    fn nearest(&self, c: usize, s: &[usize], get_costs_o_c: &Array2<f32>) -> usize {
        /*
        Compute for client c the closest object store in S
        */
        self.argmin(s, &[c], get_costs_o_c)
    }

    fn argmin(&self, s: &[usize], c: &[usize], get_costs_o_c: &Array2<f32>) -> usize {
        /*
        Compute for clients C the jointly cheapest object store in S
        */
        let mut arg = s[0];
        let mut cost_cur = self.cost_read(c, &[arg], get_costs_o_c);

        for &store in &s[1..] {
            let cost_tmp = self.cost_read(c, &[store], get_costs_o_c);
            if cost_cur > cost_tmp {
                cost_cur = cost_tmp;
                arg = store;
            }
        }

        arg
    }
}
