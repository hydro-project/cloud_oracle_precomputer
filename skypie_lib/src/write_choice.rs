use crate::object_store::ObjectStore;

#[derive(Clone,Debug,PartialEq,Eq,Hash, serde::Serialize, serde::Deserialize)]
pub struct WriteChoice {
    // List of pointers to object stores
    pub object_stores: Vec<ObjectStore>,
}

impl Default for WriteChoice {
    fn default() -> Self {
        WriteChoice {
            object_stores: Vec::new(),
        }
    }
}