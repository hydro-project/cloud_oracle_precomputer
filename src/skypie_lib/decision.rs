use crate::skypie_lib::write_choice::WriteChoice;
use crate::skypie_lib::read_choice::ReadChoice;

#[derive(Clone,PartialEq,Debug)]
pub struct Decision {
    // Write Choice
    pub write_choice: WriteChoice,
    // Read Choice
    pub read_choice: ReadChoice,
}