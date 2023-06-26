use crate::object_store::ObjectStore;

#[derive(Clone,Debug,PartialEq,Eq,Hash)]
pub struct WriteChoice {
    // List of pointers to object stores
    pub object_stores: Vec<ObjectStore>,
}