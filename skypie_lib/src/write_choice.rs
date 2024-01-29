use crate::{object_store::ObjectStore, Tombstone};

#[derive(Clone,Debug,PartialEq,Eq,Hash, serde::Serialize, serde::Deserialize)]
pub struct WriteChoice {
    // List of pointers to object stores
    pub object_stores: Vec<ObjectStore>,
}

impl Tombstone for WriteChoice {
    fn tombstone() -> Self {
        WriteChoice {
            object_stores: vec![Tombstone::tombstone(); 1],
        }
    }

    fn is_tombstone(&self) -> bool {
        self.object_stores.len() == 1 && self.object_stores[0].is_tombstone()
    }
}

impl Default for WriteChoice {
    fn default() -> Self {
        WriteChoice {
            object_stores: Vec::new(),
        }
    }
}