// import commonly used items from the prelude:
use std::fmt;
use rand::Rng;
use rand::distributions::{Uniform, Distribution};
use rand_distr::{Normal, LogNormal};

const MAX_KEY: f64 = core::f64::MAX;

#[derive(Copy, Clone)]
pub enum KeyDistribution {
    UNIFORM(rand::distributions::Uniform<f64>),
    NORMAL(rand_distr::Normal<f64>),
    LOGNORMAL(rand_distr::LogNormal<f64>),
}

impl fmt::Display for KeyDistribution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KeyDistribution::UNIFORM(_) => write!(f, "uniform"),
            KeyDistribution::NORMAL(_) => write!(f, "normal"),
            KeyDistribution::LOGNORMAL(_) => write!(f, "lognormal"),
        }
    }
}

impl KeyDistribution {
    pub fn uniform_distribution() -> KeyDistribution {
        KeyDistribution::UNIFORM(Uniform::new(0.0, MAX_KEY))
    }

    pub fn normal_distribution() -> KeyDistribution {
        KeyDistribution::NORMAL(Normal::new(5.0, 1.0).unwrap())
    }

    pub fn lognormal_distribution() -> KeyDistribution {
        KeyDistribution::LOGNORMAL(LogNormal::new(5.0, 1.0).unwrap())
    }
}

/// A random number generator
pub struct Generator {
    rand: rand::rngs::ThreadRng,
    dis: KeyDistribution,
}

impl Generator {
    pub fn new(dis: KeyDistribution) -> Self {
        Self {
            rand: rand::thread_rng(),
            dis,
        }
    }

    pub fn next_n(&mut self, n: u64) -> Vec<u64> {
        (0..n)
            .map(|_| { self.next().unwrap() })
            .collect()
    }
}

impl Iterator for Generator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let r = match self.dis {
            KeyDistribution::UNIFORM(x) => self.rand.sample(x).floor(),
            KeyDistribution::NORMAL(x) => self.rand.sample(x).floor(),
            KeyDistribution::LOGNORMAL(x) => self.rand.sample(x).floor()
        };

        Some(r as Self::Item)
    }
}

#[test]
fn test_distribution_display() {
    let uniform = KeyDistribution::uniform_distribution();

    assert_eq!(format!("{:}", uniform), "uniform");
}

#[test]
fn test_iterator() {
    let num_items :u64 = 10;
    let mut key_generator = Generator::new(KeyDistribution::uniform_distribution());
    let workload: Vec<u64> = key_generator.next_n(num_items);

    assert_eq!(workload.len(), num_items as usize);
}