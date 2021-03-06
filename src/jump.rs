use crate::util;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

/// A hashing ring implemented using jump hashing.
///
/// Jump hashing is based on using a hash of the key as the seed for a random number generator and
/// using it to jump forward in a list of nodes until it falls off the end. The last node it lands
/// on is the result.
///
/// Jump hashing is very fast and executes in `O(ln n)` time. It also has no memory overhead and has
/// virtually perfect key distribution. However, the main limitation of jump hashing is that it
/// returns an integer in the range [0, nodes) and it does not support arbitrary node names.
pub struct Ring<H = RandomState> {
    nodes: u32,
    hash_builder: H,
}

impl Ring<RandomState> {
    /// Constructs a new `Ring` with a specified number of nodes.
    pub fn new(nodes: u32) -> Self {
        Self::with_hasher(Default::default(), nodes)
    }
}

impl<H> Ring<H> {
    /// Constructs a new `Ring` with a specified number of nodes and hash builder.
    pub fn with_hasher(hash_builder: H, nodes: u32) -> Self {
        assert!(nodes >= 1);
        Self {
            hash_builder,
            nodes,
        }
    }

    /// Returns the node associated with a key.
    pub fn get_node<T>(&self, key: &T) -> u32
    where
        T: Hash,
        H: BuildHasher,
    {
        let mut h = util::gen_hash(&self.hash_builder, key);
        let mut i: i64 = -1;
        let mut j: i64 = 0;

        while j < i64::from(self.nodes) {
            i = j;
            h = h.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
            j = (((i.wrapping_add(1)) as f64) * ((1i64 << 31) as f64)
                / (((h >> 33).wrapping_add(1)) as f64)) as i64;
        }
        i as u32
    }

    /// Returns the number of nodes in the ring.
    pub fn nodes(&self) -> u32 {
        self.nodes
    }
}