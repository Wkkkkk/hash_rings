use crate::util;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::iter::Iterator;
use std::vec::Vec;

/// A hashing ring implemented using consistent hashing.
///
/// Consistent hashing is based on mapping each node to a pseudorandom value. In this
/// implementation the pseudorandom is a combination of the hash of the node and the hash of the
/// replica number. A point is also represented as a pseudorandom value and it is mapped to the
/// node with the smallest value that is greater than or equal to the point's value. If such a
/// node does not exist, then the point maps to the node with the smallest value.
pub struct Ring<'a, T, H = RandomState> {
    nodes: BTreeMap<u64, &'a T>,
    replicas: HashMap<&'a T, usize>,
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
            H: BuildHasher + Default,
    {
        Self {
            nodes: BTreeMap::new(),
            replicas: HashMap::new(),
            hash_builder,
        }
    }

    fn get_next_node(&self, hash: u64) -> Option<&T> {
        self.nodes
            .range(hash..)
            .next()
            .or_else(|| self.nodes.iter().next())
            .map(|entry| *entry.1)
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
        for i in 0..replicas {
            let hash = util::combine_hash(
                &self.hash_builder,
                util::gen_hash(&self.hash_builder, id),
                util::gen_hash(&self.hash_builder, &i),
            );
            self.nodes.insert(hash, id);
        }
        self.replicas.insert(id, replicas);
    }

    /// Removes a node and all its replicas from the ring.
    pub fn remove_node(&mut self, id: &T)
        where
            T: Hash + Eq,
            H: BuildHasher,
    {
        for i in 0..self.replicas[id] {
            let hash = util::combine_hash(
                &self.hash_builder,
                util::gen_hash(&self.hash_builder, id),
                util::gen_hash(&self.hash_builder, &i),
            );
            let should_remove = {
                if let Some(existing_id) = self.nodes.get(&hash) {
                    *existing_id == id
                } else {
                    false
                }
            };

            if should_remove {
                self.nodes.remove(&hash);
            }
        }
        self.replicas.remove(id);
    }

    /// Returns the node associated with a point.
    pub fn get_node<U>(&self, point: &U) -> &T
        where
            U: Hash,
            H: BuildHasher,
    {
        let hash = util::gen_hash(&self.hash_builder, point);
        match self.get_next_node(hash) {
            Some(node) => &*node,
            None => panic!("Error: empty ring."),
        }
    }

    fn contains_node(&self, index: u64) -> bool {
        self.nodes.contains_key(&index)
    }

    fn get_replica_count(&self, id: &T) -> usize
        where
            T: Hash + Eq,
    {
        self.replicas[id]
    }

    /// Returns the number of nodes in the ring.
    pub fn len(&self) -> usize
        where
            T: Hash + Eq,
    {
        self.replicas.len()
    }

    /// Returns `true` if the ring is empty.
    pub fn is_empty(&self) -> bool
        where
            T: Hash + Eq,
    {
        self.replicas.is_empty()
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
