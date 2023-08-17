use std::{cmp::Ordering, f64::{NEG_INFINITY, INFINITY}};

#[derive(Clone, Debug)]
pub struct Range {
    pub min: f64,
    pub max: f64
}

impl Range {
    pub fn new() -> Range {
        Range {
            min: NEG_INFINITY,
            max: INFINITY
        }
    }

    pub fn merge(&mut self, other: &Range) {
        self.min = self.min.max(other.min);
        self.max = self.max.min(other.max);
    }

    pub fn non_empty(&self) -> bool {
        self.min < self.max
    }
}

impl Eq for Range {} 

impl PartialEq for Range {
    fn eq(&self, other: &Self) -> bool {
        self.min == other.min && self.max == other.max
    }
}

impl PartialOrd for Range {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.min < other.min {
            Some(Ordering::Less)
        } else if self.min > other.min {
            Some(Ordering::Greater)
        } else if self.max < other.max {
            Some(Ordering::Less)
        } else if self.max > other.max {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Equal)
        }
    }
}

impl Ord for Range {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.min.partial_cmp(&other.min) {
            Some(Ordering::Less) => Ordering::Less,
            Some(Ordering::Greater) => Ordering::Greater,
            Some(Ordering::Equal) => match self.max.partial_cmp(&other.max) {
                Some(Ordering::Less) => Ordering::Less,
                Some(Ordering::Greater) => Ordering::Greater,
                _ => Ordering::Equal,
            },
            _ => Ordering::Equal,
        }
    }
}