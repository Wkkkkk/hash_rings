use hash_rings::{consistent, jump, carp, maglev, mpc, rendezvous, weighted_rendezvous};
use hash_rings::generator::{Generator, KeyDistribution};

use std::fs::OpenOptions;
use std::io::prelude::*;
use rand::Rng;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use rand::distributions::{Uniform, Distribution};
use rand_distr::{Normal, LogNormal};

const HASH_COUNT: u64 = 21;
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

fn write_bench_statistic(num_items: u64, num_nodes: u64, dis: KeyDistribution, throughput: f64, variances: String, latency: String, output_filename: String) {
    let output_str = format!("{}\t{}\t{:}\t{}\t{}\n", num_items, num_nodes, dis, throughput, variances);
    let file_path = format!("./src/scripts/{}.csv", output_filename);
    println!("Write to file: {}", file_path);

    let mut f = OpenOptions::new()
        .append(true)
        .create(true) // Optionally create the file if it doesn't already exist
        .open(file_path)
        .expect("Unable to open file");
    f.write_all(output_str.as_bytes()).expect("Unable to write data");

    let latency_file_name = format!("{}_{}_{:}", num_items, num_nodes, dis);
    let latency_file_path = format!("./src/scripts/{}/{}.csv", output_filename, latency_file_name);
    println!("Write to latency file: {}", latency_file_path);

    let mut l = OpenOptions::new()
        .append(true)
        .create(true) // Optionally create the file if it doesn't already exist
        .open(latency_file_path)
        .expect("Unable to open file");
    l.write_all(latency.as_bytes()).expect("Unable to write data");
}

fn bench_consistent(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching consistent hashing ({} nodes, {} replicas, {} items, {})",
        num_nodes, REPLICAS, num_items, dis
    );
    let mut rng = rand::thread_rng();
    let die = rand_distr::Normal::new(5.0, 1.0).unwrap();

    let mut occ_map = HashMap::new();
    let mut latency_map = HashMap::new();
    let mut latencies = vec![0f64; num_items as usize];

    let mut nodes = Vec::new();
    let mut ring = consistent::Ring::new();
    let total_replicas = REPLICAS * num_nodes;

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();
        occ_map.insert(id, 0f64);
        latency_map.insert(id, 0f64);
        nodes.push(id);
    }

    for node in &nodes {
        ring.insert_node(node, REPLICAS as usize);
    }

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for (i, item) in workload.iter().enumerate() {
        let id = ring.get_node(&item);
        *occ_map.get_mut(id).unwrap() += 1.0;

        // calculate latency
        let response_time = rng.sample(die);
        *latency_map.get_mut(id).unwrap() += response_time;
        let latency = *latency_map.get_mut(id).unwrap();
        latencies[i] = latency;
    }

    let variances = nodes.iter()
        .map(|node| {
            print_node_statistic(
                *node,
                REPLICAS as f64 / total_replicas as f64,
                occ_map[&node] / num_items as f64,
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, latency, String::from("consistent_hashing"));
}

fn bench_jump(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching jump hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();
    let die = rand_distr::Normal::new(5.0, 1.0).unwrap();

    let mut occ_map = HashMap::new();
    let mut latency_map = HashMap::new();
    let mut latencies = vec![0f64; num_items as usize];

    let ring = jump::Ring::new(num_nodes as u32);

    for i in 0..num_nodes {
        occ_map.insert(i, 0f64);
        latency_map.insert(i, 0f64);
    }

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for (i, item) in workload.iter().enumerate() {
        let id = ring.get_node(&item) as u64;
        *occ_map.get_mut(&id).unwrap() += 1.0;

        // calculate latency
        let response_time = rng.sample(die);
        *latency_map.get_mut(&id).unwrap() += response_time;
        let latency = *latency_map.get_mut(&id).unwrap();
        latencies[i] = latency;
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

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, latency,String::from("jump_hashing"));
}

fn bench_carp(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching carp hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();
    let die = rand_distr::Normal::new(5.0, 1.0).unwrap();

    let mut occ_map = HashMap::new();
    let mut latency_map = HashMap::new();
    let mut latencies = vec![0f64; num_items as usize];

    let mut nodes = Vec::new();
    let mut total_weight = 0f64;

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();
        let weight = rng.gen::<f64>();

        total_weight += weight;
        occ_map.insert(id, 0f64);
        latency_map.insert(id, 0f64);
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
    for (i, item) in workload.iter().enumerate() {
        let id = ring.get_node(&item);
        *occ_map.get_mut(id).unwrap() += 1.0;

        // calculate latency
        let response_time = rng.sample(die);
        *latency_map.get_mut(id).unwrap() += response_time;
        let latency = *latency_map.get_mut(id).unwrap();
        latencies[i] = latency;
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

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, latency, String::from("carp_hashing"));
}

fn bench_maglev(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching maglev hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();
    let die = rand_distr::Normal::new(5.0, 1.0).unwrap();

    let mut occ_map = HashMap::new();
    let mut latency_map = HashMap::new();
    let mut latencies = vec![0f64; num_items as usize];

    let mut nodes = Vec::new();

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();

        occ_map.insert(id, 0f64);
        latency_map.insert(id, 0f64);
        nodes.push(id);
    }

    let ring = maglev::Ring::new(nodes.iter().collect());

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for (i, item) in workload.iter().enumerate() {
        let id = ring.get_node(&item);
        *occ_map.get_mut(id).unwrap() += 1.0;

        // calculate latency
        let response_time = rng.sample(die);
        *latency_map.get_mut(id).unwrap() += response_time;
        let latency = *latency_map.get_mut(id).unwrap();
        latencies[i] = latency;
    }

    let variances = nodes.iter()
        .map(|node| {
            print_node_statistic(
                *node,
                1.0 / NODES as f64,
                occ_map[&node] as f64 / num_items as f64,
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, latency, String::from("maglev_hashing"));
}

fn bench_mpc(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching mpc hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();
    let die = rand_distr::Normal::new(5.0, 1.0).unwrap();

    let mut occ_map = HashMap::new();
    let mut latency_map = HashMap::new();
    let mut latencies = vec![0f64; num_items as usize];

    let mut nodes = Vec::new();
    let mut ring = mpc::Ring::new(HASH_COUNT);

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();

        occ_map.insert(id, 0f64);
        latency_map.insert(id, 0f64);
        nodes.push(id);
    }

    for node in &nodes {
        ring.insert_node(node);
    }

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for (i, item) in workload.iter().enumerate() {
        let id = ring.get_node(&item);
        *occ_map.get_mut(id).unwrap() += 1.0;

        // calculate latency
        let response_time = rng.sample(die);
        *latency_map.get_mut(id).unwrap() += response_time;
        let latency = *latency_map.get_mut(id).unwrap();
        latencies[i] = latency;
    }

    let variances = nodes.iter()
        .map(|node| {
            print_node_statistic(
                *node,
                1.0 / NODES as f64,
                occ_map[&node] as f64 / num_items as f64,
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, latency,String::from("mpc_hashing"));
}

fn bench_rendezvous(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching rendezvous hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();
    let die = rand_distr::Normal::new(5.0, 1.0).unwrap();

    let mut occ_map = HashMap::new();
    let mut latency_map = HashMap::new();
    let mut latencies = vec![0f64; num_items as usize];

    let mut nodes = Vec::new();
    let mut ring = rendezvous::Ring::new();

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();

        occ_map.insert(id, 0f64);
        latency_map.insert(id, 0f64);
        nodes.push(id);
    }

    for node in &nodes {
        ring.insert_node(node, 1);
    }

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for (i, item) in workload.iter().enumerate() {
        let id = ring.get_node(&item);
        *occ_map.get_mut(id).unwrap() += 1.0;

        // calculate latency
        let response_time = rng.sample(die);
        *latency_map.get_mut(id).unwrap() += response_time;
        let latency = *latency_map.get_mut(id).unwrap();
        latencies[i] = latency;
    }

    let variances = nodes.iter()
        .map(|node| {
            print_node_statistic(
                *node,
                1.0 / NODES as f64,
                occ_map[&node] / num_items as f64,
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, latency, String::from("rendezvous_hashing"));
}

fn bench_weighted_rendezvous(num_nodes: u64, num_items: u64, dis: KeyDistribution) {
    println!(
        "\nBenching weighted rendezvous hashing ({} nodes, {} items, {})",
        num_nodes, num_items, dis
    );
    let mut rng = rand::thread_rng();
    let die = rand_distr::Normal::new(5.0, 1.0).unwrap();

    let mut occ_map = HashMap::new();
    let mut latency_map = HashMap::new();
    let mut latencies = vec![0f64; num_items as usize];

    let mut nodes = Vec::new();
    let mut ring = weighted_rendezvous::Ring::new();
    let mut total_weight = 0f64;

    for _ in 0..num_nodes {
        let id = rng.gen::<u64>();
        let weight = rng.gen::<f64>();

        total_weight += weight;
        occ_map.insert(id, 0f64);
        latency_map.insert(id, 0f64);
        nodes.push((id, weight));
    }

    for node in &nodes {
        ring.insert_node(&node.0, node.1);
    }

    let mut key_generator = Generator::new(dis);
    let workload: Vec<u64> = key_generator.next_n(num_items);

    let start = Instant::now();
    for (i, item) in workload.iter().enumerate() {
        let id = ring.get_node(&item);
        *occ_map.get_mut(id).unwrap() += 1.0;

        // calculate latency
        let response_time = rng.sample(die);
        *latency_map.get_mut(id).unwrap() += response_time;
        let latency = *latency_map.get_mut(id).unwrap();
        latencies[i] = latency;
    }

    let variances = nodes.iter()
        .map(|node| {
            print_node_statistic(
                node.0,
                node.1 / total_weight as f64,
                occ_map[&node.0] / num_items as f64,
            )
        })
        .map(|v| {
            v.to_string()
        })
        .collect::<Vec<_>>()
        .join("\t");

    let throughput = print_bench_statistic(start.elapsed());

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    write_bench_statistic(num_items, num_nodes, dis, throughput, variances, latency, String::from("weighted_rendezvous_hashing"));
}

fn main() {
    let nodes_list = (5..=100).step_by(5);
    let items_list = (1000..=10_000).step_by(1000);
    // let replica_list = range(10, 100, 10);

    for nodes in nodes_list {
        for items in items_list.clone() {
            bench_consistent(nodes, items, KeyDistribution::uniform_distribution());
            bench_consistent(nodes, items, KeyDistribution::normal_distribution());
            bench_consistent(nodes, items, KeyDistribution::lognormal_distribution());

            bench_jump(nodes, items, KeyDistribution::uniform_distribution());
            bench_jump(nodes, items, KeyDistribution::normal_distribution());
            bench_jump(nodes, items, KeyDistribution::lognormal_distribution());

            bench_carp(nodes, items, KeyDistribution::uniform_distribution());
            bench_carp(nodes, items, KeyDistribution::normal_distribution());
            bench_carp(nodes, items, KeyDistribution::lognormal_distribution());

            bench_maglev(nodes, items, KeyDistribution::uniform_distribution());
            bench_maglev(nodes, items, KeyDistribution::normal_distribution());
            bench_maglev(nodes, items, KeyDistribution::lognormal_distribution());

            bench_mpc(nodes, items, KeyDistribution::uniform_distribution());
            bench_mpc(nodes, items, KeyDistribution::normal_distribution());
            bench_mpc(nodes, items, KeyDistribution::lognormal_distribution());

            bench_rendezvous(nodes, items, KeyDistribution::uniform_distribution());
            bench_rendezvous(nodes, items, KeyDistribution::normal_distribution());
            bench_rendezvous(nodes, items, KeyDistribution::lognormal_distribution());

            bench_weighted_rendezvous(nodes, items, KeyDistribution::uniform_distribution());
            bench_weighted_rendezvous(nodes, items, KeyDistribution::normal_distribution());
            bench_weighted_rendezvous(nodes, items, KeyDistribution::lognormal_distribution());
        }
    }
}
