use crate::util;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::vec::Vec;

/// A hashing ring implemented using rendezvous hashing.
///
/// Rendezvous hashing is based on based on assigning a pseudorandom value to node-point pair.
/// A point is mapped to the node that yields the greatest value associated with the node-point
/// pair. By mapping the weights to `[0, 1)` using logarithms, rendezvous hashing can be modified
/// to handle weighted nodes.
pub struct Ring<'a, T, H = RandomState> {
    nodes: HashMap<&'a T, Vec<u64>>,
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
    /// Constructs a new, empty `Ring<T>` with a specified hash builder.
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

    /// Inserts a node into the ring with a number of replicas.
    ///
    /// Increasing the number of replicas will increase the number of expected points mapped to the
    /// node. For example, a node with three replicas will receive approximately three times more
    /// points than a node with one replica.
    pub fn insert_node(&mut self, id: &'a T, replicas: usize)
        where
            T: Hash + Eq,
            H: BuildHasher,
    {
        let hashes = (0..replicas)
            .map(|index| {
                util::combine_hash(
                    &self.hash_builder,
                    util::gen_hash(&self.hash_builder, id),
                    util::gen_hash(&self.hash_builder, &index),
                )
            })
            .collect();
        self.nodes.insert(id, hashes);
    }

    /// Removes a node and all its replicas from the ring.
    pub fn remove_node(&mut self, id: &T)
        where
            T: Hash + Eq,
    {
        self.nodes.remove(id);
    }

    /// Returns the node associated with a point.
    pub fn get_node<U>(&self, id: &U) -> &'a T
        where
            T: Hash + Ord,
            U: Hash,
            H: BuildHasher,
    {
        let point_hash = util::gen_hash(&self.hash_builder, id);
        self.nodes
            .iter()
            .map(|entry| {
                (
                    entry
                        .1
                        .iter()
                        .map(|hash| util::combine_hash(&self.hash_builder, *hash, point_hash))
                        .max()
                        .expect("Expected non-zero number of replicas."),
                    entry.0,
                )
            })
            .max()
            .expect("Expected non-empty ring.")
            .1
    }

    fn get_hashes(&self, id: &T) -> Vec<u64>
        where
            T: Hash + Eq,
    {
        self.nodes[id].clone()
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