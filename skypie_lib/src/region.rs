use std::hash::Hash;

use serde::{Serialize, Deserialize};

use super::identifier::Identifier;


#[derive(Clone, Debug, Deserialize, Serialize, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct Region {
    pub id: u16,
    pub name: String,
}

impl Identifier<u16> for Region {
    fn get_id(self: &Self) -> u16 {
        self.id
    }
}

impl Default for Region {
    fn default() -> Self {
        Region{id: u16::MAX, name: "".to_string()}
    }
}