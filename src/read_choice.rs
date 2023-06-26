use crate::object_store::ObjectStore;
use crate::region::Region;

pub(crate) type ReadChoice = std::collections::HashMap<Region, ObjectStore>;
pub(crate) type ReadChoiceTuple = (Region, ObjectStore);