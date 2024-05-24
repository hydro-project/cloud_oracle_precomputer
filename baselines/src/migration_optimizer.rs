use pyo3::{prelude::*, types::PyDict};
use std::{collections::HashMap, ops::AddAssign, path::PathBuf};

use skypie_lib::{identifier::Identifier, object_store::ObjectStore, ApplicationRegion, Loader, Region, WriteChoice};

use crate::workload::Workload;

pub type WorkloadId = String;

#[pyclass(get_all)]
#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    /// The number of migrations
    pub migrations: u64,
    /// The number of rejected migrations
    pub rejected_not_worthwhile: u64,
    pub rejected_not_robust: u64,
    /// The number of objects migrated
    pub objects: u64,
}

impl AddAssign for MigrationStats {
    fn add_assign(&mut self, other: Self) {
        self.migrations += other.migrations;
        self.rejected_not_worthwhile += other.rejected_not_worthwhile;
        self.rejected_not_robust += other.rejected_not_robust;
        self.objects += other.objects;
    }
}

#[pymethods]
impl MigrationStats {
    pub fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    pub fn as_list(&self) -> Vec<(String, u64)> {
        vec![
            ("Migrations done".to_string(), self.migrations),
            ("Migrated objects".to_string(), self.objects),
            ("Migrations rejected: not worthwhile".to_string(), self.rejected_not_worthwhile),
            ("Migrations rejected: not robust".to_string(), self.rejected_not_robust),
        ]
    }
}

#[pyclass]
#[derive(Debug)]
pub struct MigrationOptimizer {
    /// Map of fully qualified object store names to object stores
    object_stores: HashMap<String, ObjectStore>,
    app_regions: HashMap<String, ApplicationRegion>,
    /// Map of workload ids to the current optimization state
    ///
    /// The optimization state is a tuple of the current decision and the accumulated loss since the last migration
    optimization_state: HashMap<WorkloadId, (WriteChoice, f64)>,
    verbose: i32,
    #[pyo3(get)]
    pub stats: MigrationStats,
}

impl MigrationOptimizer {
    /// Optimizes placement decision under dynamic workload via online optimization and maintains state for recurring optimization
    ///
    /// This function is a wrapper around `optimize_online` that maintains the optimization state for recurring optimization
    /// of workloads'/objects' placement decisions.
    /// It takes in the `workload_id` to retrieve the current placement decision and the accumulated loss since the last migration.
    /// Then it calls `optimize_online` with the retrieved state to decide wether to migrate to a new placement decision.
    /// This function maintains the optimization state for recurring optimization of the same workload, tracking the current decision and the accumulated loss.
    /// If the workload id is not found in the optimization state, it initializes the optimization state with the given optimal decision.
    /// The result of the optimization is returned as a boolean indicating wether to migrate.
    ///
    pub fn optimize_online_recurring(
        &mut self,
        workload_id: WorkloadId,
        opt: &WriteChoice,
        cur_cost: f64,
        opt_cost: f64,
        object_num: u64,
        object_size: f64,
        skip_robust: bool,
    ) -> bool {
        // Retrieve the current optimization state of the workload id
        let state = self.optimization_state.get(&workload_id);
        if let Some((cur, loss)) = state {
            if self.verbose > 1 {
                let fully_qualified_names = cur
                    .object_stores
                    .iter()
                    .map(|o| o.fully_qualified_name())
                    .collect::<Vec<_>>();
                println!(
                    "Retrieved state for id {}: loss={} for {:?}",
                    workload_id, loss, fully_qualified_names
                );
            }

            // Do the online optimization with the retrieved state
            let (migrate, loss_new, stats) = self.optimize_online(
                &cur,
                &opt,
                cur_cost,
                opt_cost,
                *loss,
                object_num,
                object_size,
                skip_robust
            );

            // Update optimization state
            let (cur, loss) = self.optimization_state.get_mut(&workload_id).unwrap();
            // Update the loss
            *loss = loss_new;
            // Update the current decision if migrating
            if migrate {
                *cur = opt.clone();
            }

            // Accumulate stats
            self.stats += stats;

            migrate
        } else {
            // Initialize the optimization state
            self.optimization_state
                .insert(workload_id, (opt.clone(), 0.0));
            true
        }
    }

    /// Optimizes placement decision under dynamic workload via online optimization
    ///
    /// ## Arguments
    /// * `cur` - The object stores of the current placement decision.
    /// * `opt` - The object stores of the currently optimal decision.
    /// * `cur_cost` - The cost of the current decision under for this "workload tick".
    /// * `opt_cost` - The cost of the optimal decision under for this "workload tick".
    /// * `loss` - The accumulated loss since the last migration.
    /// * `object_num` - The number of objects to migrate.
    /// * `object_size` - The size of the objects to migrate.
    /// 
    /// ## Description
    /// This function decides wether to migrate to a new placement decision under dynamic workload via the deterministic online optimization algorithm
    /// of "Cost Optimization for Dynamic Replication and Migration of Data in Cloud Data Centers" (https://ieeexplore.ieee.org/abstract/document/7835175)
    /// This function takes in the object stores of current decision (`cur`), the object stores of the currently optimal decision (`opt`), the current cost (`cur_cost`),
    /// the optimal cost (`opt_cost`), the accumulated loss since the last migration (`loss`),
    /// and the number of objects (`object_num`) as well as the object size (`object_size`) of the workload being migrated.
    ///
    /// It calculates the migration cost using the `minimize_migration` function and compares it with the loss and the sum of the optimized cost and migration cost.
    /// If the migration cost is less than the loss and the sum of the optimized cost and migration cost is less than the current cost,
    /// it indicates that the objects should be migrated and the loss should be reset to 0.0, returning `(true, 0.0)`.
    /// Otherwise, it indicates that the objects should not be migrated and returns the updated loss.
    ///
    /// ## Remarks
    ///
    /// Mansouri et al. designed a very conservative online algorithm to prefer the current decision over the optimal decision
    /// if the optimal cost and the migration cost exceed the current cost. As such, this online algorithm may stick with the
    /// current decisions and accumulate loss if the optimal cost and the migration cost exceed the current cost.
    ///
    /// Possibly, we should tune the conservatism, e.g., by considering the accumulated loss to some degree.
    ///
    pub fn optimize_online(
        &self,
        cur: &WriteChoice,
        opt: &WriteChoice,
        cur_cost: f64,
        opt_cost: f64,
        loss: f64,
        object_num: u64,
        object_size: f64,
        skip_robust: bool,
    ) -> (bool, f64, MigrationStats) {
        let cost_migration = self.minimize_migration(cur, opt, object_num, object_size);

        let migration_worthwhile = cost_migration < loss;
        let migration_robust = opt_cost + cost_migration < cur_cost || skip_robust;

        // Migration worthwhile and robust?
        let (do_migrate, loss) = if migration_worthwhile && migration_robust {
            if self.verbose > 0 {
                println!("Migrate!");
            }
            (true, 0.0) // Migrate the objects and reset loss
        } else {
            let loss_new = loss + (cur_cost - opt_cost);
            if self.verbose > 0 {
                if cost_migration >= loss {
                    println!("Migration not worthwhile! New loss: {}", loss_new);
                } else {
                    println!("Migration and optimal cost exceed current cost! New loss: {}", loss_new);
                }
            }
            (false, loss_new) // Do not migrate the objects and update loss
        };

        let stats = if do_migrate {
                MigrationStats {
                    migrations: 1,
                    objects: object_num,
                    ..Default::default()
                }
        } else {
                MigrationStats {
                    rejected_not_worthwhile: !migration_worthwhile as u64,
                    rejected_not_robust: !migration_robust as u64,
                    ..Default::default()
                }
        };

        (do_migrate, loss, stats)
    }

    /// Calculates the minimum migration cost from one decision to another one.
    ///
    /// # Arguments
    ///
    /// * `src` - The starting decision.
    /// * `dst` - The target decision.
    /// * `object_num` - The number of objects to migrate.
    /// * `object_size` - The size of the objects to migrate.
    ///
    /// # Returns
    ///
    /// The minimum migration cost as a floating-point number.
    ///
    fn minimize_migration(
        &self,
        src: &WriteChoice,
        dst: &WriteChoice,
        object_num: u64,
        object_size: f64,
    ) -> f64 {
        let new_object_stores = dst
            .object_stores
            .iter()
            .filter(|o| !src.object_stores.contains(o))
            .collect::<Vec<_>>();

        let cost = new_object_stores
            .into_iter()
            .map(|dst_object_store| {
                // Find minimal cost of migration from src to this new object store
                let min_cost = src
                    .object_stores
                    .iter()
                    .map(|src_object_store| {
                        (
                            src_object_store.get_migration_cost(
                                dst_object_store,
                                object_num,
                                object_size,
                            ),
                            src_object_store,
                        )
                    })
                    .inspect(|(cost, src_object_store)| {
                        if self.verbose > 2 {
                            println!(
                                "Migration cost from {} to {}: {}",
                                src_object_store.fully_qualified_name(),
                                dst_object_store.fully_qualified_name(),
                                cost
                            );
                        }
                    })
                    .reduce(|a, b| if a.0 < b.0 { a } else { b })
                    .map(|(cost, src)| {
                        if self.verbose > 1 {
                            println!(
                                "Minimal migration cost from {} to {}: {}",
                                src.fully_qualified_name(),
                                dst_object_store.fully_qualified_name(),
                                cost
                            );
                        }

                        cost
                    }); // For now keep only the cost and discard the object store

                return min_cost.unwrap();
            })
            .sum();

        return cost;
    }

    fn load(
        network_file: &str,
        object_store_file: &str,
        object_stores_considered: Vec<&str>,
        application_regions_considered: HashMap<&str, u16>,
        latency_file_path: Option<&str>,
        latency_slo: &Option<f64>,
        verbose: Option<i32>
    ) -> Loader {
        let network_file = PathBuf::from(network_file);
        let object_store_file = PathBuf::from(object_store_file);
        let object_stores_considered = object_stores_considered
            .into_iter()
            .map(|o| o.to_string())
            .collect::<Vec<_>>();
        let region_list = application_regions_considered
            .into_iter()
            .map(|(name, id)| Region {
                id,
                name: name.to_string(),
            })
            .collect::<Vec<_>>();
        let latency_file_path = latency_file_path.as_ref().map(|s| PathBuf::from(s));

        let loader = Loader::with_region_and_object_store_names(
            &network_file,
            &object_store_file,
            region_list,
            &object_stores_considered,
            &latency_file_path,
            latency_slo,
            verbose,
        );

        loader
    }
}

#[pymethods]
impl MigrationOptimizer {
    #[new]
    #[pyo3(signature = (network_file, object_store_file, object_stores_considered, application_regions_considered, latency_file_path = None, latency_slo = None, verbose = 0))]
    pub fn new(
        network_file: &str,
        object_store_file: &str,
        object_stores_considered: Vec<&str>,
        application_regions_considered: HashMap<&str, u16>,
        latency_file_path: Option<&str>,
        latency_slo: Option<f64>,
        verbose: i32,
    ) -> Self {
        let loader = Self::load(
            network_file,
            object_store_file,
            object_stores_considered,
            application_regions_considered,
            latency_file_path,
            &latency_slo,
            Some(verbose),
        );
        let object_stores = loader
            .object_stores
            .into_iter()
            .map(|o| (o.fully_qualified_name(), o))
            .collect::<HashMap<_, _>>();

        let app_regions = loader.app_regions.into_iter()
            .map(|r| (r.region.name.clone(), r))
            .collect::<HashMap<_, _>>();

        Self {
            object_stores,
            app_regions,
            optimization_state: Default::default(),
            verbose,
            stats: Default::default(),
        }
    }

    /// Optimizes placement decision under dynamic workload via online optimization, as a wrapper around `optimize_online`.
    ///
    /// This function is a wrapper around `optimize_online` that takes in the fully qualified object store names:
    /// `vendor-region-name-tier`, e.g., `aws-us-east-1-s3-General Purpose`.
    pub fn optimize_online_by_name(
        &mut self,
        cur: Vec<&str>,
        opt: Vec<&str>,
        cur_cost: f64,
        opt_cost: f64,
        loss: f64,
        object_num: u64,
        object_size: f64,
        skip_robust: bool,
    ) -> (bool, f64, MigrationStats) {
        // Translate names to object stores
        let cur = cur
            .into_iter()
            .map(|name| {
                self.object_stores
                    .get(name)
                    .expect(&format!("Object store {} not found!", &name))
                    .clone()
            })
            .collect::<Vec<_>>();
        let cur = WriteChoice { object_stores: cur };
        let opt = opt
            .into_iter()
            .map(|name| {
                self.object_stores
                    .get(name)
                    .expect(&format!("Object store {} not found!", &name))
                    .clone()
            })
            .collect::<Vec<_>>();
        let opt = WriteChoice { object_stores: opt };

        self.optimize_online(
            &cur,
            &opt,
            cur_cost,
            opt_cost,
            loss,
            object_num,
            object_size,
            skip_robust
        )
    }

    /// Optimizes placement decision under dynamic workload via online optimization and maintains state for recurring optimization, as a wrapper around `optimize_online_recurring`.
    ///
    /// This function is a wrapper around `optimize_online_recurring` that takes in the fully qualified object store names:
    /// `vendor-region-name-tier`, e.g., `aws-us-east-1-s3-General Purpose`.
    pub fn optimize_online_recurring_by_name(
        &mut self,
        workload_id: WorkloadId,
        opt: Vec<&str>,
        cur_cost: f64,
        opt_cost: f64,
        object_num: u64,
        object_size: f64,
        skip_robust: bool,
    ) -> bool {
        // Translate names to object stores
        let opt = opt
            .into_iter()
            .map(|name| {
                self.object_stores
                    .get(name)
                    .expect(&format!("Object store {} not found!", &name))
                    .clone()
            })
            .collect::<Vec<_>>();
        let opt = WriteChoice { object_stores: opt };

        self.optimize_online_recurring(
            workload_id,
            &opt,
            cur_cost,
            opt_cost,
            object_num,
            object_size,
            skip_robust
        )
    }

    /// Compute the cost of the workload under the given placement
    /// 
    /// ## Arguments
    /// * `workload` - The workload to compute the cost for.
    /// * `write_choice` - The object stores of the placement, as fully qualitifed names.
    /// * `read_choice` - The assigment of application regions to object stores of the placement, as dictionary of application region name and fully qualified object store name.
    pub fn cost<'py>(&self, workload: Workload, write_choice: Vec<&str>, read_choice: &'py PyDict) -> f64 {
        
        let write_cost: f64 = write_choice.into_iter().map(|name|{
            // Enfores SkyPIE's naming convention, replacing : with -
            let name = name.replace(":", "-");
            let object_store = self.object_stores
                .get(&name)
                .expect(&format!("Object store {} not found!", &name));

            let cost = object_store.cost.size_cost * workload.size
                + object_store.cost.put_cost * workload.puts;

            return cost;
        }).sum();

        let read_cost: f64 = read_choice.iter().map(|(key, value)|{
            let app_region_name = key.extract::<&str>().unwrap().replace(":", "-");
            let object_store_name = value.extract::<&str>().unwrap().replace(":", "-");
            let app_region = self.app_regions.get(&app_region_name).expect(format!("Application region {} not found!", &app_region_name).as_str());
            let object_store = self.object_stores.get(&object_store_name).expect(format!("Object store {} not found!", &object_store_name).as_str());

            let app_region_id = app_region.get_id() as usize;
            let cost = object_store.get_ingress_cost(app_region) * workload.get_ingress(app_region_id)
                + object_store.compute_read_costs(app_region, workload.get_gets(app_region_id), workload.get_egress(app_region_id));

            return cost;
        }).sum();

        write_cost + read_cost
    }

    pub fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}
