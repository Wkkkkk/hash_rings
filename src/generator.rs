// import commonly used items from the prelude:
use rand::Rng;
use rand::distributions::{Uniform, Distribution};
use rand_distr::{Normal, LogNormal};

const MAX_KEY: f64 = 10.0;
const MEDIUM_KEY: f64 = MAX_KEY / 2.0;

pub struct Generator {
    rand: rand::rngs::ThreadRng,
    uniform: rand::distributions::Uniform<f64>,
    normal: rand_distr::Normal<f64>,
    lognormal: rand_distr::LogNormal<f64>,
}

impl Generator {
    pub fn new() -> Self {
        Self {
            rand: rand::thread_rng(),
            uniform: Uniform::new(0.0, MAX_KEY),
            normal: Normal::new(MEDIUM_KEY, 1.0).unwrap(),
            lognormal: LogNormal::new(MEDIUM_KEY, 1.0).unwrap(),
        }
    }

    pub fn next(&mut self) -> u64 {
        self.rand.sample(self.normal).floor() as u64
    }

    pub fn next_n(&mut self, n: u64) -> Vec<u64> {
        (0..n)
            .map(|_| { self.next() })
            .collect()
    }
}