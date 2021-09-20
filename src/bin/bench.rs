use hash_rings::consistent;
use hash_rings::generator;

use rand::Rng;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const REPLICAS: u64 = 100;
const ITEMS: u64 = 100_000;
const NODES: u64 = 10;

fn print_node_statistic(id: u64, expected: f64, actual: f64) {
    let error = (expected - actual) / expected;
    println!(
        "{:020} - Expected: {:.6} | Actual: {:.6} | Error: {:9.6}",
        id, expected, actual, error,
    );
}

fn print_bench_statistic(duration: Duration) {
    let total_time = duration.as_secs() as f64 * 1e9 + f64::from(duration.subsec_nanos());
    let ns_per_op = total_time / ITEMS as f64;
    let ops_per_ns = 1e9 / ns_per_op;
    println!();
    println!("Total elapsed time:         {:>10.3} ms", total_time / 1e6);
    println!("Milliseconds per operation: {:>10.3} ns", ns_per_op);
    println!("Operations per second:      {:>10.3} op/ms", ops_per_ns);
    println!();
}

fn bench_consistent() {
    println!(
        "\nBenching consistent hashing ({} nodes, {} replicas, {} items)",
        NODES, REPLICAS, ITEMS,
    );
    let mut rng = rand::thread_rng();

    let mut occ_map = HashMap::new();
    let mut nodes = Vec::new();
    let mut ring = consistent::Ring::new();
    let total_replicas = REPLICAS * NODES;

    for _ in 0..NODES {
        let id = rng.gen::<u64>();
        occ_map.insert(id, 0f64);
        nodes.push(id);
    }

    for node in &nodes {
        ring.insert_node(node, REPLICAS as usize);
    }

    let mut g = generator::Generator::new();
    let workload: Vec<u64> = g.next_n(ITEMS);

    let start = Instant::now();
    for key in workload {
        let id = ring.get_node(&key);
        *occ_map.get_mut(id).unwrap() += 1.0;
    }

    for node in &nodes {
        print_node_statistic(
            *node,
            REPLICAS as f64 / total_replicas as f64,
            occ_map[&node] / ITEMS as f64,
        );
    }
    print_bench_statistic(start.elapsed());
}

fn main() {
    bench_consistent();
}
