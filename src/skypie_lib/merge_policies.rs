use itertools::Itertools;

pub use crate::ApplicationRegion;
use crate::decision::Decision;
use crate::object_store::ObjectStore;
use crate::read_choice::ReadChoice;
use crate::write_choice::WriteChoice;

use super::decision::DecisionRef;
use super::read_choice::ReadChoiceRef;

pub(crate) type Assignments = std::collections::HashMap<ApplicationRegion, Vec<(f64, ObjectStore)>>;
pub(crate) type AssignmentsRef<'a> = std::collections::HashMap<&'a ApplicationRegion, Vec<(f64, ObjectStore)>>;
//type Assignments_ref<'a> = std::collections::HashMap<Region, Vec<(f64, &'a ObjectStore)>>;
//type AssignmentsStream = std::vec::IntoIter<(Region, (f64, Box<crate::object_store::ObjectStoreStruct>))>;

pub(crate) struct MergeIterator {
    iter: std::vec::IntoIter<(f64, (ApplicationRegion, ObjectStore))>,
    cur_s: f64,
    cur_a: ReadChoice,
    write_choice: Box<WriteChoice>,
}

impl MergeIterator {
    pub fn new(write_choice: Box<WriteChoice>, assignments: Assignments) -> MergeIterator {
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

    #[allow(non_snake_case)]
    fn initialize_with_assignment(
        assignments: &mut Assignments,
    ) -> ReadChoice {
        assert_ne!(assignments.len(), 0);
    
        let mut cur_A = ReadChoice::new(assignments.len());
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
    ) -> std::vec::IntoIter<(f64, (ApplicationRegion, ObjectStore))> {
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
            // XXX: Materializing the write set here!
            while let Some((s, (r, o))) = self.iter.next() {
                if s > self.cur_s {
                    let decision = Some(Decision {
                        write_choice: (*self.write_choice).clone(),
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
                write_choice: (*self.write_choice).clone(),
                read_choice: self.cur_a.clone(),
            });

            self.cur_a.clear();
            return decision;
        }
    }
}

pub(crate) struct MergeIteratorRef<'a> {
    iter: std::vec::IntoIter<(f64, (&'a ApplicationRegion, ObjectStore))>,
    cur_s: f64,
    cur_a: ReadChoiceRef::<'a>, //std::collections::HashMap<&'a ApplicationRegion, ObjectStore>,
    write_choice: Box<WriteChoice>,
}

impl<'a> MergeIteratorRef<'a> {
    pub fn new(write_choice: Box<WriteChoice>, assignments: AssignmentsRef<'a>) -> MergeIteratorRef<'a> {
        debug_assert_ne!(assignments.len(), 0);
        // Make assignments mutable
        //let mut assignments = assignments;
        let cur_s = -1.0;
        let (cur_a, assignments_rest) = Self::initialize_with_assignment(assignments);
        let iter = Self::priority_queue(assignments_rest);
        MergeIteratorRef {
            iter,
            cur_s,
            cur_a,
            write_choice: write_choice,
        }
    }

    #[allow(non_snake_case)]
    fn initialize_with_assignment<'b>(
        mut assignments: AssignmentsRef<'b>,
    ) -> (ReadChoiceRef::<'b>, AssignmentsRef<'b>) {
        assert_ne!(assignments.len(), 0);
    
        type ThisHashMap<'b> = ReadChoiceRef::<'b>;
        let mut cur_A = ThisHashMap::new(assignments.len());
        for (r, l) in assignments.iter_mut() {
            debug_assert_ne!(l.len(), 0);
    
            // Find index of tuple with minimum first element
            let min_index = l
                .iter()
                .enumerate()
                .min_by(|x, y| x.1 .0.partial_cmp(&y.1 .0).unwrap())
                .unwrap()
                .0;
            // Remove tuple with minimum first element and insert into cur_A
            let min = l.remove(min_index);
    
            cur_A.insert(r, min.1.clone());
        }
        (cur_A, assignments)
    }
    
    // Convert and sort assignments into a priority queue
    //fn priority_queue(assignments: Assignments) -> Vec<(f64, ReadChoiceTuple)> {
    fn priority_queue<'b>(
        assignments: AssignmentsRef::<'b>,
    ) -> std::vec::IntoIter<(f64, (&'b ApplicationRegion, ObjectStore))> {
        let list = assignments
            .into_iter()
            .flat_map(|(r, l)| l.into_iter().map(move |(up, o)| (up, (r, o))));
    
        let sorted = list.sorted_by(|x, y| x.0.partial_cmp(&y.0).unwrap());
        sorted
    }
}

impl<'a> Iterator for MergeIteratorRef<'a> {
    type Item = DecisionRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_a.is_empty() {
            return None;
        } else {
            while let Some((s, (r, o))) = self.iter.next() {
                if s > self.cur_s {
                    let decision = Some(DecisionRef::<'a> {
                        write_choice: self.write_choice.clone(),
                        read_choice: self.cur_a.clone(),
                    });

                    self.cur_a.insert(r, o.clone());
                    self.cur_s = s;

                    return decision;
                }
                {
                    self.cur_a.insert(r, o.clone());
                }
            }
            // Emit last policy and indicate end by clearing cur_A
            let decision = Some(DecisionRef::<'a> {
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
    use std::collections::HashMap;

    use crate::{object_store::{self, ObjectStore}, merge_policies::AssignmentsRef, ApplicationRegion, Region};
    extern crate test;
    //use itertools::Itertools;
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
        let regions: Vec<Region> = (0..read_choice_size).map(|x| Region{id: x, name: x.to_string()}).collect();

        let assignments = regions.iter().map(|r| {
            let list: Vec<(f64, ObjectStore)> = write_choice.clone()
                .object_stores
                .into_iter()
                .map(|o: ObjectStore| (o.id as f64, o))
                .collect();
            
            assert_eq!(list.len(), write_choice_size as usize);
            let app_region = ApplicationRegion{region: r.clone(), egress_cost: HashMap::default(), ingress_cost: HashMap::default()};

            (app_region, list)
        });

        let assignments = Assignments::from_iter(assignments);

        b.iter(|| {
            let merge_iterator = super::MergeIterator::new(Box::<super::WriteChoice>::new(write_choice.clone()), assignments.clone());
            
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

        let regions: Vec<Region> = vec![
            Region{id: 0, name: "1".to_string()},
            Region{id: 1, name: "2".to_string()},
            Region{id: 2, name: "3".to_string()},
        ];

        let assignments = regions.iter().map(|r| {
            let list: Vec<(f64, ObjectStore)> = write_choice.clone()
                .object_stores
                .into_iter()
                .map(|o| (o.id as f64, o))
                .collect();
            let app_region = ApplicationRegion{region: r.clone(), egress_cost: HashMap::default(), ingress_cost: HashMap::default()};
            
            (app_region, list)
        });

        let assignments = Assignments::from_iter(assignments);

        let merge_iterator = super::MergeIterator::new(Box::<super::WriteChoice>::new(write_choice.clone()), assignments);

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

    #[test]
    fn test_merge_iterator_ref() {
        let object_stores: Vec<object_store::ObjectStore> = vec![
            object_store::ObjectStoreStruct::new(1),
            object_store::ObjectStoreStruct::new(2),
            object_store::ObjectStoreStruct::new(3),
        ];

        let write_choice = super::WriteChoice {
            object_stores: object_stores,
        };

        let regions: Vec<Region> = vec![
            Region{id: 0, name: "1".to_string()},
            Region{id: 1, name: "2".to_string()},
            Region{id: 2, name: "3".to_string()},
        ];

        let assignments = regions.iter().map(|r| {
            let list: Vec<(f64, ObjectStore)> = write_choice.clone()
                .object_stores
                .into_iter()
                .map(|o| (o.id as f64, o))
                .collect();
            let app_region = ApplicationRegion{region: r.clone(), egress_cost: HashMap::default(), ingress_cost: HashMap::default()};
            
            (app_region, list)
        }).collect_vec();

        let assignments_ref = AssignmentsRef::from_iter(assignments.iter().map(|(r, l)| (r, l.clone())));

        //let merge_iterator = super::Merge::new(Box::<super::WriteChoice>::new(write_choice.clone()), assignments);
        let merge_iterator = super::MergeIteratorRef::new(Box::<super::WriteChoice>::new(write_choice.clone()), assignments_ref);

        let decisions = merge_iterator.collect::<Vec<super::DecisionRef>>();

        assert_eq!(decisions.len(), 3);
        // Check decisions
        for (i, decision) in decisions.iter().enumerate() {
            assert_eq!(decision.write_choice.object_stores.len(), 3);
            assert_eq!(*decision.write_choice, write_choice);

            // Check read choice
            assert_eq!(decision.read_choice.len(), 3);
            for (_r, o) in decision.read_choice.iter() {
                assert_eq!(o.id as usize, i + 1);
            }
        }
    }
}
