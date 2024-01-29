pub trait Tombstone {
    // Construct a tombstone. The implementation should care for ambiguity!
    fn tombstone() -> Self;
    // Check if this is a tombstone
    fn is_tombstone(&self) -> bool;
}