struct Combinations<'a, T> {
    pool: &'a [T],
    indices: Vec<usize>,
    first: bool,
}

impl<'a, T> Combinations<'a, T> {
    fn new(pool: &'a [T], r: usize) -> Self {
        Self {
            pool,
            indices: (0..r).collect(),
            first: true,
        }
    }
}

impl<'a, T> Iterator for Combinations<'a, T> {
    type Item = Vec<&'a T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.first {
            self.first = false;
        } else {
            let mut i = self.indices.len() - 1;
            while self.indices[i] == self.pool.len() - self.indices.len() + i {
                if i == 0 {
                    return None;
                }
                i -= 1;
            }

            self.indices[i] += 1;
            for j in i + 1..self.indices.len() {
                self.indices[j] = self.indices[j - 1] + 1;
            }
        }

        Some(self.indices.iter().map(|&i| &self.pool[i]).collect())
    }
}

pub fn combinations_recursive_bulk<'a, T: Clone + Copy>(pool: &[T], r: usize) -> Vec<Vec<T>> {
    if r == 0 {
        return vec![vec![]];
    }

    if r == pool.len() {
        return vec![pool.to_vec()];
    }

    let mut res = combinations_recursive_bulk(&pool[1..], r);
    for mut c in combinations_recursive_bulk(&pool[1..], r - 1) {
        c.push(pool[0]);
        res.push(c);
    }

    res
}

pub fn combinations_recursive_closure<'a, T: Clone + Copy, F>(pool: &[T], r: usize, f: &mut F )
    where F: FnMut(Vec<T>) {

    for (i, o) in pool.iter().enumerate() {
        let mut temp_combination = vec![*o];
        if temp_combination.len() == r {
            f(temp_combination);
        } else {
            combinations_recursive_closure_internal(&pool[i+1..], r, f, &mut temp_combination);
        }
    }
}

fn combinations_recursive_closure_internal<'a, T: Clone + Copy, F>(pool: &[T], r: usize, f: &mut F, temp_combination: &mut Vec<T>)
    where F: FnMut(Vec<T>) {

    for (i, o) in pool.iter().enumerate() {
        temp_combination.push(*o);
        if temp_combination.len() == r {
            f(temp_combination.clone());
        } else {
            combinations_recursive_closure_internal(&pool[i+1..], r, f, temp_combination);
        }
        temp_combination.pop();
    }
}

/* 
pub async fn combinations_recursive_closure_parallel<'a, T: Clone + Copy, F>(pool: &[T], r: usize, f: F )
    where F: FnMut(Vec<T>) {

    let futures = pool.iter().enumerate().map(|(i, o)| {
        let temp_combination = vec![*o];
        combinations_recursive_closure_internal_async(&pool[i+1..], r, f, temp_combination)
    });

    // Sync
    for f in futures {
        f.await;
    }
}

async fn combinations_recursive_closure_internal_async<'a, T: Clone + Copy, F>(pool: &[T], r: usize, mut f: F, mut temp_combination: Vec<T>)
    where F: FnMut(Vec<T>) {

    for (i, o) in pool.iter().enumerate() {
        temp_combination.push(*o);
        if temp_combination.len() == r {
            f(temp_combination.clone());
        } else {
            combinations_recursive_closure_internal(&pool[i+1..], r, f, temp_combination);
        }
        temp_combination.pop();
    }
} */


#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use test::Bencher;

    use crate::{
        network_record::NetworkCostMap,
        object_store::{Cost, ObjectStore, ObjectStoreStruct},
        region::Region,
    };

    use super::{combinations_recursive_bulk, Combinations, combinations_recursive_closure};
    extern crate test;
    //use test::Bencher;

    #[test]
    fn test_combinator() {
        let replication_factor = 2;

        let mut cost1 = Cost::new(10.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(vec![(
            Region {
                name: "0".to_string(),
            },
            1.0,
        )]);
        cost1.add_egress_costs(egress_cost);
        let o1 = ObjectStore::new(ObjectStoreStruct {
            id: 0,
            cost: cost1,
            region: Region {
                name: "".to_string(),
            },
            name: "".to_string(),
        });

        let mut cost2 = Cost::new(2.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(vec![(
            Region {
                name: "0".to_string(),
            },
            2.0,
        )]);
        cost2.add_egress_costs(egress_cost);
        let o2 = ObjectStore::new(ObjectStoreStruct {
            id: 1,
            cost: cost2,
            region: Region {
                name: "".to_string(),
            },
            name: "".to_string(),
        });

        let mut cost3 = Cost::new(2.0, "get request");
        let egress_cost = NetworkCostMap::from_iter(vec![(
            Region {
                name: "0".to_string(),
            },
            2.0,
        )]);
        cost3.add_egress_costs(egress_cost);
        let o3 = ObjectStore::new(ObjectStoreStruct {
            id: 2,
            cost: cost3,
            region: Region {
                name: "".to_string(),
            },
            name: "".to_string(),
        });

        let object_stores = vec![o1.clone(), o2.clone(), o3.clone()];

        let combinator = Combinations::new(&object_stores, replication_factor);
        let combinations: Vec<Vec<&Box<ObjectStoreStruct>>> = combinator.collect();
        println!("{:?}", combinations);

        assert_eq!(combinations.len(), 3);
    }

    enum TestType {
        Recursive,
        RecursiveClosure,
        Iterative,
        Itertools,
    }

    fn bench_combinations_n(
        b: &mut Bencher,
        no_object_stores: u16,
        replication_factor: usize,
        bulk: TestType,
    ) {
        let no_object_stores = test::black_box(no_object_stores);
        let object_stores: Vec<ObjectStore> = (0..no_object_stores)
            .map(|i| {
                let mut cost = Cost::new(10.0, "get request");
                let egress_cost = NetworkCostMap::from_iter(vec![(
                    Region {
                        name: "0".to_string(),
                    },
                    1.0,
                )]);
                cost.add_egress_costs(egress_cost);
                let o = ObjectStore::new(ObjectStoreStruct {
                    id: i,
                    cost: cost,
                    region: Region {
                        name: "".to_string(),
                    },
                    name: "".to_string(),
                });
                return o;
            })
            .collect();

        let object_store_refs: Vec<&ObjectStore> = object_stores.iter().map(|o| o).collect();

        b.iter(|| {
            let replication_factor = test::black_box(replication_factor);


            match bulk {
                TestType::Iterative => {
                    let combinator = Combinations::new(&object_store_refs, replication_factor);
                    let res: usize = combinator.fold(0, |agg, x| {
                        let _x = test::black_box(x);
                        return agg + 1;
                    });
                    return res;
                }
                TestType::Recursive => {
                    let res = combinations_recursive_bulk(&object_store_refs, replication_factor);
                    let res: usize = res.into_iter().fold(0, |agg, x| {
                        let _x = test::black_box(x);
                        return agg + 1;
                    });
                    return res;
                }
                TestType::Itertools => {
                    let res = object_store_refs.iter().combinations(replication_factor);
                    let res: usize = res.fold(0, |agg, x| {
                        let _x = test::black_box(x);
                        return agg + 1;
                    });
                    return res;
                }
                TestType::RecursiveClosure => {
                    let mut res = 0;
                    let mut f = |x: Vec<&ObjectStore>| {
                        let _x = test::black_box(x);
                        res += 1;
                    };
                    combinations_recursive_closure(&object_store_refs, replication_factor, &mut f);
                    return res;
                }
                _ => {
                    panic!("bulk must be 0 or 1");
                }
            }
            
        });
    }

    const NO_OBJECT_STORES: u16 = 200;
    #[bench]
    fn bench_combinations_2_iterative(b: &mut Bencher) {
        let replication_factor = 2;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Iterative);
    }
    #[bench]
    fn bench_combinations_2_recursive(b: &mut Bencher) {
        let replication_factor = 2;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Recursive);
    }
    #[bench]
    fn bench_combinations_2_itertools(b: &mut Bencher) {
        let replication_factor = 2;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Itertools);
    }
    #[bench]
    fn bench_combinations_2_closure(b: &mut Bencher) {
        let replication_factor = 2;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::RecursiveClosure);
    }

    #[bench]
    fn bench_combinations_3_iterative(b: &mut Bencher) {
        let replication_factor = 3;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Iterative);
    }
    #[bench]
    fn bench_combinations_3_recursive(b: &mut Bencher) {
        let replication_factor = 3;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Recursive);
    }
    #[bench]
    fn bench_combinations_3_itertools(b: &mut Bencher) {
        let replication_factor = 3;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Itertools);
    }

    #[bench]
    fn bench_combinations_3_closure(b: &mut Bencher) {
        let replication_factor = 3;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::RecursiveClosure);
    }
    #[bench]
    fn bench_combinations_4_iterative(b: &mut Bencher) {
        let replication_factor = 4;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Iterative);
    }
    #[bench]
    fn bench_combinations_4_itertools(b: &mut Bencher) {
        let replication_factor = 4;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Itertools);
    }
    #[bench]
    fn bench_combinations_4_closure(b: &mut Bencher) {
        let replication_factor = 4;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::RecursiveClosure);
    }

    #[bench]
    fn bench_combinations_5_iterative(b: &mut Bencher) {
        let replication_factor = 5;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Iterative);
    }
    #[bench]
    fn bench_combinations_5_itertools(b: &mut Bencher) {
        let replication_factor = 5;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::Itertools);
    }
    #[bench]
    fn bench_combinations_5_closure(b: &mut Bencher) {
        let replication_factor = 5;
        bench_combinations_n(b, NO_OBJECT_STORES, replication_factor, TestType::RecursiveClosure);
    }
}
