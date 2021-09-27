use hash_rings::{consistent, jump, carp};
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
    let error = (expected - actual).abs() / expected;
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

fn write_bench_statistic(num_items: u64, num_nodes: u64, dis: KeyDistribution, throughput: f64, variances: String, output_filename: String) {
    let output_str = format!("{}\t{}\t{:}\t{}\t{}\n", num_items, num_nodes, dis, throughput, variances);
    let file_path = format!("./src/scripts/{}.csv", output_filename);
    println!("Write to file: {}", file_path);

    let mut f = OpenOptions::new()
        .append(true)
        .create(true) // Optionally create the file if it doesn't already exist
        .open(file_path)
        .expect("Unable to open file");
    f.write_all(output_str.as_bytes()).expect("Unable to write data");
}

fn bench_consistent(num_nodes: u64, num_replicas: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching consistent hashing ({} nodes, {} replicas, {} items, {})",
        num_nodes, num_replicas, num_items, dis
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

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, String::from("consistent_hashing"));
}

fn bench_jump(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching jump hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();

    let mut occ_map = HashMap::new();
    let ring = jump::Ring::new(num_nodes as u32);

    for i in 0..num_nodes {
        occ_map.insert(i, 0f64);
    }

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for item in workload {
        let id = ring.get_node(&item) as u64;
        *occ_map.get_mut(&id).unwrap() += 1.0;
    }

    let variances = (0..num_nodes)
        .map(|i| {
            print_node_statistic(
                i,
                1.0 / num_nodes as f64,
                occ_map[&i] / num_items as f64
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, String::from("jump_hashing"));
}

fn bench_carp(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching carp hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();

    let mut occ_map = HashMap::new();
    let mut nodes = Vec::new();
    let mut total_weight = 0f64;

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();
        let weight = rng.gen::<f64>();

        total_weight += weight;
        occ_map.insert(id, 0f64);
        nodes.push((id, weight));
    }

    let ring = carp::Ring::new(
        nodes
            .iter()
            .map(|node| carp::Node::new(&node.0, node.1))
            .collect(),
    );

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
                node.0,
                node.1 / total_weight,
                occ_map[&node.0] / num_items as f64,
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, String::from("carp_hashing"));
}

fn main() {
    let nodes_list = (5..=100).step_by(5);
    let items_list = (1000..=10_000).step_by(1000);
    // let replica_list = range(10, 100, 10);

    for nodes in nodes_list {
        for items in items_list.clone() {
            bench_consistent(nodes, REPLICAS, items, KeyDistribution::uniform_distribution());
            bench_consistent(nodes, REPLICAS, items, KeyDistribution::normal_distribution());
            bench_consistent(nodes, REPLICAS, items, KeyDistribution::lognormal_distribution());

            bench_jump(nodes, items, KeyDistribution::uniform_distribution());
            bench_jump(nodes, items, KeyDistribution::normal_distribution());
            bench_jump(nodes, items, KeyDistribution::lognormal_distribution());

            bench_carp(nodes, items, KeyDistribution::uniform_distribution());
            bench_carp(nodes, items, KeyDistribution::normal_distribution());
            bench_carp(nodes, items, KeyDistribution::lognormal_distribution());
        }
    }
}
