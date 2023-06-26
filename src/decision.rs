use crate::write_choice::WriteChoice;
use crate::read_choice::ReadChoice;

#[derive(Clone,PartialEq,Debug)]
pub struct Decision {
    // Write Choice
    pub write_choice: WriteChoice,
    // Read Choice
    pub read_choice: ReadChoice,
}