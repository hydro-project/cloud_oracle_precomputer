use crate::{object_store::ObjectStore, ApplicationRegion};
pub trait CompatibilityChecker {
    fn is_compatible(&self, object_store: &ObjectStore, app: &ApplicationRegion) -> bool;
}

pub struct DefaultCompatibilityChecker {
}

impl CompatibilityChecker for DefaultCompatibilityChecker {
    fn is_compatible(&self, _object_store: &ObjectStore, _app: &ApplicationRegion) -> bool {
        true
    }
}