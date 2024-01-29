use std::fmt;

#[derive(Clone)]
pub struct MonitorMovingAverage {
    size: usize,
    count: usize,
    last: std::time::Instant,
    arrival_times: Vec<std::time::Duration>,
}

impl MonitorMovingAverage {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            count: 0,
            last: std::time::Instant::now(),
            arrival_times: Vec::with_capacity(size),
        }
    }

    fn add_arrival_time(&mut self, time: std::time::Instant) {
        let pos = self.count % self.size;
        let arrival_time = time - self.last;

        if pos < self.arrival_times.len() {
            self.arrival_times[pos] = arrival_time;
        } else {
            self.arrival_times.push(arrival_time);
        }

        self.last = time;
        self.count += 1;
    }

    pub fn add_arrival_time_now(&mut self) {
        let now = std::time::Instant::now();
        self.add_arrival_time(now);
    }

    pub fn add_arrival_time_now_sampled(&mut self, sample_rate: i32) {
        if self.count % sample_rate as usize == 0 {
            self.add_arrival_time_now();
        }
        else {
            self.count += 1;
        }
    }

    pub fn get_arrival_time_average(&self) -> Option<std::time::Duration> {
        if self.arrival_times.is_empty() {
            return None;
        }

        let sum: std::time::Duration = self.arrival_times.iter().sum();
        let average = sum / self.arrival_times.len() as u32;

        Some(average)
    }

    pub fn get_throughput(&self) -> Option<f64> {
        
        let average = self.get_arrival_time_average().unwrap();
        let average = average.as_secs_f64();
        let average = 1.0 / average;

        Some(average)
    }

    pub fn get_count(&self) -> usize {
        self.count
    }

    pub fn print(&self, prefix: &str, sample_rate: Option<i32>) {
        if let Some(rate) = sample_rate {
            if self.count % rate as usize == 0 {
                println!("{}{}", prefix, self);
            }
        } else {
            println!("{}{}", prefix, self);
        }
    }

}

impl fmt::Display for MonitorMovingAverage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let throughput = self.get_throughput().unwrap();
        let count = self.get_count();
        write!(f, "{} items, {:.2} items/s", count, throughput)
    }
}

#[derive(Clone)]
pub struct MonitorNOOP {
}

impl MonitorNOOP {
    pub fn new(_size: usize) -> Self {
        Self {}
    }

    pub fn add_arrival_time_now(&mut self) {
    }

    pub fn add_arrival_time_now_sampled(&mut self, _sample_rate: i32) {
    }

    pub fn get_arrival_time_average(&self) -> Option<std::time::Duration> {
        None
    }

    pub fn get_throughput(&self) -> Option<f64> {
        None
    }

    pub fn get_count(&self) -> usize {
        0
    }

    pub fn print(&self, _prefix: &str, _sample_rate: Option<i32>) {
    }

}

impl fmt::Display for MonitorNOOP {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Result::Ok(())
    }
}