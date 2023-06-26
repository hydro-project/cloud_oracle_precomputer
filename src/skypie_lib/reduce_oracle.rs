use itertools::Itertools;

use crate::skypie_lib::decision::Decision;

pub struct Batcher<Iter>
where Iter: Iterator<Item = Decision>
{
    batch_size: usize,
    candidates: Iter
}

impl<Iter> Batcher<Iter>
where Iter: Iterator<Item = Decision>
{
    pub fn new(batch_size: usize, candidates: Iter) -> Batcher::<Iter>{
        Batcher { batch_size, candidates}
    }   
}

// Implement iterator for ReduceOracle
impl<Iter> Iterator for Batcher<Iter>
where Iter: Iterator<Item = Decision>
{
    type Item = Vec<Decision>;

    fn next(&mut self) -> Option<Self::Item> {
        
        let mut cur_size = 0;
        let mut cur_batch = Self::Item::new();
        cur_batch.reserve(self.batch_size);

        loop {
            let item = self.candidates.next();
            let is_some = item.is_some();
            
            if is_some {
                cur_batch.push(item.unwrap());
                cur_size += 1;
            }

            if cur_size == 0 {
                return None;
            }
            
            if cur_size == self.batch_size || !is_some {
                
                // Consume a batch of decisions
    
                return Some(cur_batch);
            }
        }
    }
}


/* pub fn reduce_oracle<'a, Iter>(candidates: Iter, batch_size: usize) -> Iter
where Iter: Iterator<Item = Decision<'a>>
{
    // Pull in a batch of candidates
    let batches = Batcher::new(batch_size, candidates);
    
    // Consume a batch of decisions
    // Dummy
    let reduced = batches.into_iter().map(|batch| {
        batch.split_off(0)
    }).flatten();
} */