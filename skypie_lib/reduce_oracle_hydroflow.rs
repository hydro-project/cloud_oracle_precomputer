// Collect batch of decisions
pub struct BatcherMap<T> {
    batch_size: usize,
    batch: Vec<T>
}

impl<T> BatcherMap<T>
{
    pub fn new(batch_size: usize) -> BatcherMap::<T>{
        BatcherMap::<T>{ batch_size, batch: Self::allocate_batch(batch_size)}
    }

    fn allocate_batch(batch_size: usize) -> Vec<T> {
        Vec::with_capacity(batch_size)
    }

    pub fn add(&mut self, elem: T) -> Option<Vec<T>> {

        self.batch.push(elem);
        if self.batch.len() == self.batch_size {
            let mut batch = Self::allocate_batch(self.batch_size);
            std::mem::swap(&mut batch, &mut self.batch);
            return Some(batch);
        }
        else {
            return None;
        }
    }

    pub fn get_batch(&self) -> &Vec<T> {
        &self.batch
    }

    // Fixme: Loosing items of the last incomplete batch, new do drain at the end!
}