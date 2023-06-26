use itertools::Itertools;

use crate::Decision;
use crate::ObjectStore;
use crate::ReadChoice;
use crate::Region;
use crate::WriteChoice;

pub(crate) type Assignments = std::collections::HashMap<Region, Vec<(f64, ObjectStore)>>;
//type Assignments_ref<'a> = std::collections::HashMap<Region, Vec<(f64, &'a ObjectStore)>>;
//type AssignmentsStream = std::vec::IntoIter<(Region, (f64, Box<crate::object_store::ObjectStoreStruct>))>;

pub(crate) struct MergeIterator {
    iter: std::vec::IntoIter<(f64, (Region, ObjectStore))>,
    cur_s: f64,
    cur_a: std::collections::HashMap<Region, ObjectStore>,
    write_choice: WriteChoice,
}

impl<'a> MergeIterator {
    pub fn new(write_choice: WriteChoice, assignments: Assignments) -> MergeIterator {
        assert_ne!(assignments.len(), 0);
        // Make assignments mutable
        let mut assignments = assignments;
        let cur_s = -1.0;
        let cur_a = Self::initialize_with_assignment(&mut assignments);
        let iter = Self::priority_queue(assignments);
        MergeIterator {
            iter,
            cur_s,
            cur_a,
            write_choice,
        }
    }

    fn initialize_with_assignment(
        assignments: &mut Assignments,
    ) -> std::collections::HashMap<Region, ObjectStore> {
        assert_ne!(assignments.len(), 0);

        let mut cur_A = ReadChoice::new();
        for (r, l) in assignments {
            debug_assert_ne!(l.len(), 0);

            // Find index of tuple with mimium first element
            let min_index = l
                .iter()
                .enumerate()
                .min_by(|x, y| x.1 .0.partial_cmp(&y.1 .0).unwrap())
                .unwrap()
                .0;
            // Remove tuple with minimum first element and insert into cur_A
            let min = l.remove(min_index);

            cur_A.insert(r.clone(), min.1.clone());
        }
        cur_A
    }

    // Convert and sort assignments into a priority queue
    //fn priority_queue(assignments: Assignments) -> Vec<(f64, ReadChoiceTuple)> {
    fn priority_queue(
        assignments: Assignments,
    ) -> std::vec::IntoIter<(f64, (Region, ObjectStore))> {
        let list = assignments
            .into_iter()
            .flat_map(|(r, l)| l.into_iter().map(move |(up, o)| (up, (r.clone(), o))));

        let sorted = list.sorted_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
        sorted
    }
}

impl Iterator for MergeIterator {
    type Item = Decision;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_a.is_empty() {
            return None;
        } else {
            while let Some((s, (r, o))) = self.iter.next() {
                if s > self.cur_s {
                    let decision = Some(Decision {
                        write_choice: self.write_choice.clone(),
                        read_choice: self.cur_a.clone(),
                    });

                    self.cur_a.insert(r.clone(), o.clone());
                    self.cur_s = s;

                    return decision;
                }
                {
                    self.cur_a.insert(r.clone(), o.clone());
                }
            }
            // Emit last policy and indicate end by clearing cur_A
            let decision = Some(Decision {
                write_choice: self.write_choice.clone(),
                read_choice: self.cur_a.clone(),
            });

            self.cur_a.clear();
            return decision;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::object_store::{self, ObjectStore};
    extern crate test;
    use test::Bencher;

    use super::Assignments;

    #[bench]
    fn bench_merge_iterator(b: &mut Bencher) {
        let write_choice_size = 5;
        let read_choice_size = 200;

        let write_choice_size = test::black_box(write_choice_size);
        let object_stores: Vec<object_store::ObjectStore> = (0..write_choice_size).map(|x| object_store::ObjectStoreStruct::new(x)).collect();
        //let object_stores_ref: Vec<&ObjectStore> = Vec::from_iter(object_stores.iter().map(|x: &ObjectStore| {return x;}));

        let write_choice = super::WriteChoice {
            object_stores: object_stores,
        };

        let read_choice_size = test::black_box(read_choice_size);
        let regions: Vec<super::Region> = (0..read_choice_size).map(|x| super::Region{name: x.to_string()}).collect();

        let assignments = regions.iter().map(|r| {
            let list: Vec<(f64, ObjectStore)> = write_choice.clone()
                .object_stores
                .into_iter()
                .map(|o: ObjectStore| (o.id as f64, o))
                .collect();
            
            assert_eq!(list.len(), write_choice_size as usize);

            (r.clone(), list)
        });

        let assignments = Assignments::from_iter(assignments);

        b.iter(|| {
            let merge_iterator = super::MergeIterator::new(write_choice.clone(), assignments.clone());
            
            let decisions = merge_iterator.collect::<Vec<super::Decision>>();
            return decisions;
        });
    }

    #[test]
    fn test_merge_iterator() {
        let object_stores: Vec<object_store::ObjectStore> = vec![
            object_store::ObjectStoreStruct::new(1),
            object_store::ObjectStoreStruct::new(2),
            object_store::ObjectStoreStruct::new(3),
        ];

        let write_choice = super::WriteChoice {
            object_stores: object_stores,
        };

        let regions: Vec<super::Region> = vec![
            super::Region{name: "1".to_string()},
            super::Region{name: "2".to_string()},
            super::Region{name: "3".to_string()},
        ];

        let assignments = regions.iter().map(|r| {
            let list: Vec<(f64, ObjectStore)> = write_choice.clone()
                .object_stores
                .into_iter()
                .map(|o| (o.id as f64, o))
                .collect();
            (r.clone(), list)
        });

        let assignments = Assignments::from_iter(assignments);

        let merge_iterator = super::MergeIterator::new(write_choice.clone(), assignments);

        let decisions = merge_iterator.collect::<Vec<super::Decision>>();

        assert_eq!(decisions.len(), 3);
        // Check decisions
        for (i, decision) in decisions.iter().enumerate() {
            assert_eq!(decision.write_choice.object_stores.len(), 3);
            assert_eq!(decision.write_choice, write_choice);

            // Check read choice
            assert_eq!(decision.read_choice.len(), 3);
            for (_r, o) in decision.read_choice.iter() {
                assert_eq!(o.id as usize, i + 1);
            }
        }
    }
}
