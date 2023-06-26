use crate::skypie_lib::object_store::ObjectStore;
use crate::skypie_lib::region::Region;

pub(crate) type ReadChoice = std::collections::HashMap<Region, ObjectStore>;
pub(crate) type ReadChoiceTuple = (Region, ObjectStore);