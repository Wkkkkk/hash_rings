use crate::util;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::vec::Vec;

/// A hashing ring implemented using weighted rendezvous hashing.
///
/// Rendezvous hashing is based on based on assigning a pseudorandom value to node-point pair.
/// A point is mapped to the node that yields the greatest value associated with the node-point
/// pair.
pub struct Ring<'a, T, H = RandomState> {
    nodes: HashMap<&'a T, f64>,
    hash_builder: H,
}

impl<'a, T> Ring<'a, T, RandomState> {
    /// Constructs a new, empty `Ring<T>`.
    pub fn new() -> Self
        where
            T: Hash + Eq,
    {
        Self::default()
    }
}

impl<'a, T, H> Ring<'a, T, H> {
    /// Constructs a new, empty `Ring<T>` with a specified hash builder;
    pub fn with_hasher(hash_builder: H) -> Self
        where
            T: Hash + Eq,
            H: BuildHasher,
    {
        Self {
            nodes: HashMap::new(),
            hash_builder,
        }
    }

    /// Inserts a node into the ring with a particular weight.
    ///
    /// Increasing the weight will increase the number of expected points mapped to the node. For
    /// example, a node with a weight of three will receive approximately three times more points
    /// than a node with a weight of one.
    pub fn insert_node(&mut self, id: &'a T, weight: f64)
        where
            T: Hash + Eq,
    {
        self.nodes.insert(id, weight);
    }

    /// Removes a node from the ring.
    pub fn remove_node(&mut self, id: &T)
        where
            T: Hash + Eq,
    {
        self.nodes.remove(id);
    }

    /// Returns the node associated with a point.
    pub fn get_node<U>(&self, point: &U) -> &'a T
        where
            T: Hash + Ord,
            U: Hash,
            H: BuildHasher,
    {
        let point_hash = util::gen_hash(&self.hash_builder, point);
        self.nodes
            .iter()
            .map(|entry| {
                let hash = util::combine_hash(
                    &self.hash_builder,
                    util::gen_hash(&self.hash_builder, entry.0),
                    point_hash,
                );
                (
                    -entry.1 / (hash as f64 / u64::max_value() as f64).ln(),
                    entry.0,
                )
            })
            .max_by(|n, m| {
                if n == m {
                    n.1.cmp(m.1)
                } else {
                    n.0.partial_cmp(&m.0).expect("Expected all non-NaN floats.")
                }
            })
            .expect("Expected non-empty ring.")
            .1
    }

    /// Returns the number of nodes in the ring.
    pub fn len(&self) -> usize
        where
            T: Hash + Eq,
    {
        self.nodes.len()
    }

    /// Returns `true` if the ring is empty.
    pub fn is_empty(&self) -> bool
        where
            T: Hash + Eq,
    {
        self.nodes.is_empty()
    }
}

impl<'a, T, H> Default for Ring<'a, T, H>
    where
        T: Hash + Eq,
        H: BuildHasher + Default,
{
    fn default() -> Self {
        Self::with_hasher(Default::default())
    }
}