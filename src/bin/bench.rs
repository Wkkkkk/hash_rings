use hash_rings::consistent;
use hash_rings::generator::{Generator, KeyDistribution};

use std::fs::OpenOptions;
use std::io::prelude::*;
use rand::Rng;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const REPLICAS: u64 = 100;
const ITEMS: u64 = 100_000;
const NODES: u64 = 10;

fn print_node_statistic(id: u64, expected: f64, actual: f64) -> f64 {
    let error = (expected - actual) / expected;
    println!(
        "{:020} - Expected: {:.6} | Actual: {:.6} | Error: {:9.6}",
        id, expected, actual, error,
    );
    error
}

fn print_bench_statistic(duration: Duration) -> f64 {
    let total_time = duration.as_secs() as f64 * 1e9 + f64::from(duration.subsec_nanos());
    let ns_per_op = total_time / ITEMS as f64;
    let ops_per_ns = 1e9 / ns_per_op;
    println!();
    println!("Total elapsed time:         {:>10.3} ms", total_time / 1e6);
    println!("Milliseconds per operation: {:>10.3} ns", ns_per_op);
    println!("Operations per second:      {:>10.3} op/ms", ops_per_ns);
    println!();
    ops_per_ns
}

fn write_result()

fn bench_consistent(num_nodes: u64, num_replicas: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching consistent hashing ({} nodes, {} replicas, {} items)",
        num_nodes, num_replicas, num_items,
    );
    let mut rng = rand::thread_rng();

    let mut occ_map = HashMap::new();
    let mut nodes = Vec::new();
    let mut ring = consistent::Ring::new();
    let total_replicas = num_replicas * num_nodes;

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();
        occ_map.insert(id, 0f64);
        nodes.push(id);
    }

    for node in &nodes {
        ring.insert_node(node, num_replicas as usize);
    }

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for item in workload {
        let id = ring.get_node(&item);
        *occ_map.get_mut(id).unwrap() += 1.0;
    }

    let variances = nodes.iter()
        .map(|node| {
            print_node_statistic(
                *node,
                num_replicas as f64 / total_replicas as f64,
                occ_map[&node] / num_items as f64,
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    let output_str = format!("{}\t{}\t{:}\t{}\t{}\n", num_items, num_nodes, dis, throughput, variances);
    let mut f = OpenOptions::new()
        .append(true)
        .create(true) // Optionally create the file if it doesn't already exist
        .open("./src/scripts/consistent_hashing.csv")
        .expect("Unable to open file");
    f.write_all(output_str.as_bytes()).expect("Unable to write data");
}

fn main() {
    let nodes_list = 3..=20;
    let items_list = (1000..=10_000).step_by(1000);
    // let replica_list = range(10, 100, 10);

    for nodes in nodes_list {
        for items in items_list.clone() {
            bench_consistent(nodes, REPLICAS, items, KeyDistribution::uniform_distribution());
            bench_consistent(nodes, REPLICAS, items, KeyDistribution::normal_distribution());
            bench_consistent(nodes, REPLICAS, items, KeyDistribution::lognormal_distribution());
        }
    }
}
