use crate::util;
use std::collections::hash_map::RandomState;
use std::f64;
use std::hash::{BuildHasher, Hash};

/// A node with an associated weight.
///
/// The distribution of points to nodes is proportional to the weights of the nodes. For example, a
/// node with a weight of 3 will receive approximately three times more points than a node with a
/// weight of 1.
pub struct Node<'a, T> {
    id: &'a T,
    hash: u64,
    weight: f64,
    relative_weight: f64,
}

impl<'a, T> Node<'a, T> {
    /// Constructs a new node with a particular weight associated with it.
    pub fn new(id: &'a T, weight: f64) -> Self {
        Node {
            id,
            hash: 0,
            weight,
            relative_weight: 0f64,
        }
    }
}

/// A hashing ring implemented using the Cache Array Routing Protocol.
///
/// The Cache Array Routing Protocol calculates the relative weight for each node in the ring to
/// distribute points according to their weights.
pub struct Ring<'a, T, H = RandomState> {
    nodes: Vec<Node<'a, T>>,
    hash_builder: H,
}

impl<'a, T> Ring<'a, T, RandomState> {
    /// Constructs a new, empty `Ring<T>`.
    pub fn new(nodes: Vec<Node<'a, T>>) -> Self
    where
        T: Hash + Ord,
    {
        Self::with_hasher(Default::default(), nodes)
    }
}

impl<'a, T, H> Ring<'a, T, H> {
    fn rebalance(&mut self) {
        let mut product = 1f64;
        let len = self.nodes.len() as f64;
        for i in 0..self.nodes.len() {
            let index = i as f64;
            let mut res;
            if i == 0 {
                res = (len * self.nodes[i].weight).powf(1f64 / len);
            } else {
                res = (len - index) * (self.nodes[i].weight - self.nodes[i - 1].weight) / product;
                res += self.nodes[i - 1].relative_weight.powf(len - index);
                res = res.powf(1f64 / (len - index));
            }

            product *= res;
            self.nodes[i].relative_weight = res;
        }
        if let Some(max_relative_weight) = self.nodes.last().map(|node| node.relative_weight) {
            for node in &mut self.nodes {
                node.relative_weight /= max_relative_weight
            }
        }
    }

    /// Constructs a new, empty `Ring<T>` with a specified hash builder.
    pub fn with_hasher(hash_builder: H, mut nodes: Vec<Node<'a, T>>) -> Self
    where
        T: Hash + Ord,
        H: BuildHasher + Default,
    {
        for node in &mut nodes {
            node.hash = util::gen_hash(&hash_builder, node.id);
        }
        nodes.reverse();
        nodes.sort_by_key(|node| node.id);
        nodes.dedup_by_key(|node| node.id);
        nodes.sort_by(|n, m| {
            if (n.weight - m.weight).abs() < f64::EPSILON {
                n.id.cmp(m.id)
            } else {
                n.weight
                    .partial_cmp(&m.weight)
                    .expect("Expected all non-NaN floats.")
            }
        });
        let mut ret = Self {
            nodes,
            hash_builder,
        };
        ret.rebalance();
        ret
    }

    /// Inserts a node into the ring with a particular weight.
    ///
    /// Increasing the weight will increase the number of expected points mapped to the node. For
    /// example, a node with a weight of three will receive approximately three times more points
    /// than a node with a weight of one.
    pub fn insert_node(&mut self, mut new_node: Node<'a, T>)
    where
        T: Hash + Ord,
        H: BuildHasher,
    {
        new_node.hash = util::gen_hash(&self.hash_builder, new_node.id);
        if let Some(index) = self.nodes.iter().position(|node| node.id == new_node.id) {
            self.nodes[index] = new_node;
        } else {
            self.nodes.push(new_node);
        }
        self.nodes.sort_by(|n, m| {
            if (n.weight - m.weight).abs() < f64::EPSILON {
                n.id.cmp(m.id)
            } else {
                n.weight
                    .partial_cmp(&m.weight)
                    .expect("Expected all non-NaN floats.")
            }
        });
        self.rebalance();
    }

    /// Removes a node from the ring.
    pub fn remove_node(&mut self, id: &T)
    where
        T: Eq,
    {
        if let Some(index) = self.nodes.iter().position(|node| node.id == id) {
            self.nodes.remove(index);
            self.rebalance();
        }
    }

    /// Returns the node associated with a point.
    pub fn get_node<U>(&self, point: &U) -> &'a T
    where
        T: Ord,
        U: Hash,
        H: BuildHasher,
    {
        let point_hash = util::gen_hash(&self.hash_builder, point);
        self.nodes
            .iter()
            .map(|node| {
                (
                    util::combine_hash(&self.hash_builder, node.hash, point_hash) as f64
                        * node.relative_weight,
                    node.id,
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
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}