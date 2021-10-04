use crate::util;
use rand::Rng;
use siphasher::sip::SipHasher;
use std::collections::hash_map::RandomState;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash, Hasher};

const PRIME: u64 = 0xFFFF_FFFF_FFFF_FFC5;

/// A hashing ring implemented using multi-probe consistent hashing.
///
/// Multi-probe consistent hashing is a variation on consistent hashing where instead of the nodes
/// being hashed multiple times to reduce variance, the keys are hashed multiple times. Each key is
/// hashed `hash_count` times and the closest node over all hashes is returned.
pub struct Ring<'a, T, H = RandomState> {
    nodes: BTreeMap<u64, &'a T>,
    hash_count: u64,
    hashers: [SipHasher; 2],
    hash_builder: H,
}

impl<'a, T> Ring<'a, T, RandomState> {
    /// Constructs a new, empty `Ring<T>` that hashes `hash_count` times when a key is inserted.
    pub fn new(hash_count: u64) -> Self {
        assert!(hash_count > 0);
        Self {
            nodes: BTreeMap::new(),
            hash_count,
            hashers: Self::get_hashers(),
            hash_builder: Default::default(),
        }
    }
}

impl<'a, T, H> Ring<'a, T, H> {
    fn get_hashers() -> [SipHasher; 2] {
        let mut rng = rand::thread_rng();
        [
            SipHasher::new_with_keys(rng.gen::<u64>(), rng.gen::<u64>()),
            SipHasher::new_with_keys(rng.gen::<u64>(), rng.gen::<u64>()),
        ]
    }

    fn get_hashes<U>(&self, item: &U) -> [u64; 2]
        where
            U: Hash,
    {
        let mut ret = [0; 2];
        for (index, hash) in ret.iter_mut().enumerate() {
            let mut sip = self.hashers[index];
            item.hash(&mut sip);
            *hash = sip.finish();
        }
        ret
    }

    fn get_distance(hash: u64, next_hash: u64) -> u64 {
        if hash > next_hash {
            next_hash + (<u64>::max_value() - hash)
        } else {
            next_hash - hash
        }
    }

    fn get_next_hash(&self, hash: u64) -> u64 {
        let next_hash_opt = self
            .nodes
            .range(hash..)
            .next()
            .or_else(|| self.nodes.iter().next())
            .map(|entry| *entry.0);
        match next_hash_opt {
            Some(hash) => hash,
            None => panic!("Error: empty ring."),
        }
    }

    /// Constructs a new, empty `Ring<T>` that hashes `hash_count` times when a key is inserted
    /// with a specified hash builder.
    pub fn with_hasher(hash_builder: H, hash_count: u64) -> Self {
        assert!(hash_count > 0);
        Self {
            nodes: BTreeMap::new(),
            hash_count,
            hashers: Self::get_hashers(),
            hash_builder,
        }
    }

    /// Inserts a node into the ring with a number of replicas.
    ///
    /// Increasing the number of replicas will increase the number of expected points mapped to the
    /// node. For example, a node with three replicas will receive approximately three times more
    /// points than a node with one replica.
    pub fn insert_node(&mut self, id: &'a T)
        where
            T: Hash,
            H: BuildHasher,
    {
        self.nodes
            .insert(util::gen_hash(&self.hash_builder, id), id);
    }

    /// Removes a node.
    pub fn remove_node(&mut self, id: &T)
        where
            T: Hash,
            H: BuildHasher,
    {
        self.nodes.remove(&util::gen_hash(&self.hash_builder, id));
    }

    /// Returns the node associated with a point.
    pub fn get_node<U>(&self, point: &U) -> &T
        where
            U: Hash,
    {
        let hashes = self.get_hashes(point);
        let hash = (0..self.hash_count)
            .map(|i| {
                let hash = hashes[0].wrapping_add((i as u64).wrapping_mul(hashes[1]) % PRIME);
                let next_hash = self.get_next_hash(hash);
                (Self::get_distance(hash, next_hash), next_hash)
            })
            .min()
            .expect("Error: expected positive hash count.");

        self.nodes[&hash.1]
    }

    /// Returns the number of nodes in the ring.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}
