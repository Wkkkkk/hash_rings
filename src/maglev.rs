use primal::Sieve;
use rand::Rng;
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};
use std::iter;

/// A hashing ring implemented using maglev hashing.
///
/// Maglev hashing produces a lookup table that allows finding a node in constant time by
/// generating random permutations.
pub struct Ring<'a, T> {
    nodes: Vec<&'a T>,
    lookup: Vec<usize>,
    hasher: SipHasher,
}

impl<'a, T> Ring<'a, T> {
    fn get_hashers() -> [SipHasher; 2] {
        let mut rng = rand::thread_rng();
        [
            SipHasher::new_with_keys(rng.gen::<u64>(), rng.gen::<u64>()),
            SipHasher::new_with_keys(rng.gen::<u64>(), rng.gen::<u64>()),
        ]
    }

    /// Constructs a new `Ring<T>` with a specified list of nodes.
    pub fn new(nodes: Vec<&'a T>) -> Self
        where
            T: Hash,
    {
        assert!(!nodes.is_empty());
        let capacity_hint = nodes.len() * 100;
        Ring::with_capacity_hint(nodes, capacity_hint)
    }

    /// Constructs a new `Ring<T>` with a specified list of nodes and a capacity hint. The actual
    /// capacity of the ring will always be the next prime greater than or equal to
    /// `capacity_hint`. If nodes are removed and the ring is regenerated, the ring should be
    /// rebuilt with the same capacity.
    pub fn with_capacity_hint(nodes: Vec<&'a T>, capacity_hint: usize) -> Self
        where
            T: Hash,
    {
        let hashers = Self::get_hashers();
        let lookup = Self::populate(&hashers, &nodes, capacity_hint);
        Self {
            nodes,
            lookup,
            hasher: hashers[0],
        }
    }

    fn get_hash<U>(hasher: SipHasher, key: &U) -> usize
        where
            U: Hash,
    {
        let mut sip = hasher;
        key.hash(&mut sip);
        sip.finish() as usize
    }

    fn populate(hashers: &[SipHasher; 2], nodes: &[&T], capacity_hint: usize) -> Vec<usize>
        where
            T: 'a + Hash,
    {
        let m = Sieve::new(capacity_hint * 2)
            .primes_from(capacity_hint)
            .next()
            .expect("Expected a prime larger than or equal to `capacity_hint`.");
        let n = nodes.len();

        let permutation: Vec<Vec<usize>> = nodes
            .iter()
            .map(|node| {
                let offset = Self::get_hash(hashers[0], node) % m;
                let skip = (Self::get_hash(hashers[1], node) % (m - 1)) + 1;
                (0..m).map(|i| (offset + i * skip) % m).collect()
            })
            .collect();

        let mut next: Vec<usize> = iter::repeat(0).take(n).collect();
        let mut entry: Vec<usize> = iter::repeat(<usize>::max_value()).take(m).collect();

        let mut i = 0;
        while i < m {
            for j in 0..n {
                let mut c = permutation[j][next[j]];
                while entry[c] != <usize>::max_value() {
                    next[j] += 1;
                    c = permutation[j][next[j]];
                }
                entry[c] = j;
                next[j] += 1;
                i += 1;

                if i == m {
                    break;
                }
            }
        }

        entry
    }

    /// Returns the number of nodes in the ring.
    pub fn nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the capacity of the ring. If nodes are removed and the ring is regenerated, the
    /// ring should be rebuilt with the same capacity.
    pub fn capacity(&self) -> usize {
        self.lookup.len()
    }

    /// Returns the node associated with a key.
    pub fn get_node<U>(&self, key: &U) -> &T
        where
            U: Hash,
    {
        let index = Self::get_hash(self.hasher, key) % self.capacity();
        self.nodes[self.lookup[index]]
    }
}
