use itertools::Itertools;

use crate::object_store::ObjectStore;
use crate::ApplicationRegion;

use super::identifier::Identifier;

pub type ReadChoice = ReadChoiceVec<ApplicationRegion>;
pub type ReadChoiceRef<'a> = ReadChoiceVec<&'a ApplicationRegion>;
pub type ReadChoiceIter<'b> = ReadChoiceVecIter<'b, ApplicationRegion>;
pub type ReadChoiceRefIter<'b, 'a> = ReadChoiceVecIter<'b, &'a ApplicationRegion>;
//pub type ReadChoiceRefIter<'a,'b>

pub type ReadChoiceHash = std::collections::HashMap<ApplicationRegion, ObjectStore>;
pub type ReadChoiceRefHash<'a> = std::collections::HashMap<&'a ApplicationRegion, ObjectStore>;
//pub(crate) type ReadChoiceTuple = (Region, ObjectStore);

pub type ReadChoiceVecIter<'b, K> = std::slice::Iter<'b, (K, ObjectStore)>;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, Clone)]
pub struct ReadChoiceVec<K> {
    read_choices: Vec<(K, ObjectStore)>,
}

impl<K> ReadChoiceVec<K>
where
    K: Default + Identifier<u16> + PartialEq + std::clone::Clone,
{
    pub fn new(num_apps: usize) -> ReadChoiceVec<K> {
        ReadChoiceVec {
            read_choices: vec![(K::default(), ObjectStore::default()); num_apps],
        }
    }

    pub fn len(&self) -> usize {
        self.read_choices.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, (K, ObjectStore)> {
        self.read_choices.iter()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let pos = key.get_id() as usize;
        return self.read_choices[pos].0 == *key;
    }

    pub fn get(&self, key: &K) -> ObjectStore {
        let pos = key.get_id() as usize;
        return self.read_choices[pos].1.clone();
    }

    pub fn insert(&mut self, key: K, value: ObjectStore) {
        let pos = key.get_id() as usize;
        debug_assert!(pos < self.read_choices.len(), "ID {} out-of-bounds!", pos);
        self.read_choices[pos] = (key, value);
    }

    pub fn is_empty(&self) -> bool {
        self.read_choices.is_empty()
    }

    pub fn clear(&mut self) {
        self.read_choices.clear();
    }
}

impl<K> FromIterator<(K, ObjectStore)> for ReadChoiceVec<K> {
    fn from_iter<T: IntoIterator<Item = (K, ObjectStore)>>(iter: T) -> Self {
        let read_choices = iter.into_iter().collect_vec();
        ReadChoiceVec { read_choices }
    }
}

impl<K> Default for ReadChoiceVec<K> {
    fn default() -> Self {
        ReadChoiceVec {
            read_choices: Vec::default(),
        }
    }
}
