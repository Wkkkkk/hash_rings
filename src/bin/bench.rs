use hash_rings::{consistent, jump, carp, maglev, mpc, rendezvous, weighted_rendezvous};
use hash_rings::generator::{Generator, KeyDistribution};
use hash_rings::util;

use std::fs::OpenOptions;
use std::io::prelude::*;
use rand::Rng;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use rand::distributions::{Uniform, Distribution};
use rand_distr::{Normal, LogNormal};

const HASH_COUNT: u64 = 21;
const REPLICAS: u64 = 10;
const ITEMS: u64 = 100_000;
const NODES: u64 = 10;

fn mean(data: &[f64]) -> Option<f64> {
    let sum = data.iter().sum::<f64>();
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

fn std_deviation(data: &[f64]) -> Option<f64> {
    match (mean(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data.iter().map(|value| {
                let diff = data_mean - (*value as f64);

                diff * diff
            }).sum::<f64>() / count as f64;

            Some(variance.sqrt())
        },
        _ => None
    }
}

fn print_node_statistic(id: u64, expected: f64, actual: f64) -> f64 {
    let error = (expected - actual).abs() / expected;
    println!(
        "{:020} - Expected: {:.6} | Actual: {:.6} | Error: {:9.6}",
        id, expected, actual, error,
    );
    error
}

fn print_bench_statistic(num_items : u64, duration: Duration) -> f64 {
    let total_time = duration.as_secs() as f64 * 1e9 + f64::from(duration.subsec_nanos());
    let ns_per_op = total_time / num_items as f64;
    let ops_per_ns = 1e9 / ns_per_op;
    println!();
    println!("Total elapsed time:         {:>10.3} ms", total_time / 1e6);
    println!("Milliseconds per operation: {:>10.3} ns", ns_per_op);
    println!("Operations per second:      {:>10.3} op/ms", ops_per_ns);
    println!();
    ops_per_ns
}

fn print_std_error(num_nodes: u64, variances: &[f64]) -> (f64, String){
    let std_error = std_deviation(variances).unwrap();
    let confidence_interval = 2.576 * std_error / (num_nodes as f64).sqrt();
    let left  = 1.0 - confidence_interval / 2.0;
    let right = 1.0 + confidence_interval / 2.0;
    let confidence_interval = format!("({}, {})", left, right);

    (std_error, confidence_interval)
}

fn write_bench_statistic(num_items: u64, num_nodes: u64, dis: KeyDistribution, throughput: f64, std_error: f64, confidence_interval: String, latency: String, output_filename: String) {
    let output_str = format!("{}\t{}\t{}\t{:}\t{}\t{}\t{}\n", num_items, num_nodes, num_items/num_nodes, dis, throughput, std_error, confidence_interval);
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

    let throughput = print_bench_statistic(num_items, start.elapsed());

    let variances = nodes.iter()
        .map(|node| {
            let actual_load = occ_map[&node] / num_items as f64;
            let expected_load = 1.0 / num_nodes as f64;
            actual_load/expected_load
        })
        .collect::<Vec<_>>();

    let (std_error, confidence_interval) = print_std_error(num_nodes, &variances);

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    write_bench_statistic(num_items, num_nodes, dis, throughput, std_error, confidence_interval, latency, String::from("consistent_hashing"));
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

    let throughput = print_bench_statistic(num_items, start.elapsed());

    let variances = (0..num_nodes)
        .map(|i| {
            let actual_load = occ_map[&i] / num_items as f64;
            let expected_load = 1.0 / num_nodes as f64;
            actual_load/expected_load
        })
        .collect::<Vec<_>>();

    let (std_error, confidence_interval) = print_std_error(num_nodes, &variances);

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    write_bench_statistic(num_items, num_nodes, dis, throughput, std_error, confidence_interval, latency, String::from("jump_hashing"));
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

    let throughput = print_bench_statistic(num_items, start.elapsed());

    let variances = nodes.iter()
        .map(|node| {
            let actual_load = occ_map[&node] / num_items as f64;
            let expected_load = 1.0 / num_nodes as f64;
            actual_load/expected_load
        })
        .collect::<Vec<_>>();

    let (std_error, confidence_interval) = print_std_error(num_nodes, &variances);

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    write_bench_statistic(num_items, num_nodes, dis, throughput, std_error, confidence_interval, latency, String::from("maglev_hashing"));
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

    let throughput = print_bench_statistic(num_items, start.elapsed());

    let variances = nodes.iter()
        .map(|node| {
            let actual_load = occ_map[&node] / num_items as f64;
            let expected_load = 1.0 / num_nodes as f64;
            actual_load/expected_load
        })
        .collect::<Vec<_>>();

    let (std_error, confidence_interval) = print_std_error(num_nodes, &variances);

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    write_bench_statistic(num_items, num_nodes, dis, throughput, std_error, confidence_interval, latency, String::from("mpc_hashing"));
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

    let throughput = print_bench_statistic(num_items, start.elapsed());

    let variances = nodes.iter()
        .map(|node| {
            let actual_load = occ_map[&node] / num_items as f64;
            let expected_load = 1.0 / num_nodes as f64;
            actual_load/expected_load
        })
        .collect::<Vec<_>>();

    let (std_error, confidence_interval) = print_std_error(num_nodes, &variances);

    let latency = latencies.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    write_bench_statistic(num_items, num_nodes, dis, throughput, std_error, confidence_interval, latency, String::from("rendezvous_hashing"));
}

fn print_vec(items: &[u64], output_filename: String) {
    let str = items.iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("\t");

    let mut f = OpenOptions::new()
        .append(true)
        .create(true) // Optionally create the file if it doesn't already exist
        .open(output_filename)
        .expect("Unable to open file");
    f.write_all(str.as_bytes()).expect("Unable to write data");
}

fn print_workload() {
    let num_keys = 10_000u64;

    {
        let hash_builder = std::collections::hash_map::RandomState::default();
        let mut key_generator = Generator::new(KeyDistribution::uniform_distribution());
        let workload: Vec<u64> = key_generator.next_n(num_keys);
        let hashed_keys: Vec<u64> = workload.iter()
            .map(|key| { util::gen_hash(&hash_builder, key) })
            .collect();

        print_vec(&workload, String::from("./src/scripts/uniform_workload.csv"));
        print_vec(&hashed_keys, String::from("./src/scripts/hashed_uniform_workload.csv"));
    }
    {
        let hash_builder = std::collections::hash_map::RandomState::default();
        let mut key_generator = Generator::new(KeyDistribution::normal_distribution());
        let workload: Vec<u64> = key_generator.next_n(num_keys);
        let hashed_keys: Vec<u64> = workload.iter()
            .map(|key| { util::gen_hash(&hash_builder, key) })
            .collect();

        print_vec(&workload, String::from("./src/scripts/normal_workload.csv"));
        print_vec(&hashed_keys, String::from("./src/scripts/hashed_normal_workload.csv"));
    }
    {
        let hash_builder = std::collections::hash_map::RandomState::default();
        let mut key_generator = Generator::new(KeyDistribution::lognormal_distribution());
        let workload: Vec<u64> = key_generator.next_n(num_keys);
        let hashed_keys: Vec<u64> = workload.iter()
            .map(|key| { util::gen_hash(&hash_builder, key) })
            .collect();

        print_vec(&workload, String::from("./src/scripts/lognormal_workload.csv"));
        print_vec(&hashed_keys, String::from("./src/scripts/hashed_lognormal_workload.csv"));
    }
}

fn main() {
    print_workload();

    let nodes_list = (10..=200).step_by(10);
    let items_list = (1000..=50_000).step_by(1000);

    for nodes in nodes_list {
        for items in items_list.clone() {
            bench_consistent(nodes, items, KeyDistribution::uniform_distribution());
            bench_consistent(nodes, items, KeyDistribution::normal_distribution());
            bench_consistent(nodes, items, KeyDistribution::lognormal_distribution());

            bench_jump(nodes, items, KeyDistribution::uniform_distribution());
            bench_jump(nodes, items, KeyDistribution::normal_distribution());
            bench_jump(nodes, items, KeyDistribution::lognormal_distribution());

            bench_maglev(nodes, items, KeyDistribution::uniform_distribution());
            bench_maglev(nodes, items, KeyDistribution::normal_distribution());
            bench_maglev(nodes, items, KeyDistribution::lognormal_distribution());

            bench_mpc(nodes, items, KeyDistribution::uniform_distribution());
            bench_mpc(nodes, items, KeyDistribution::normal_distribution());
            bench_mpc(nodes, items, KeyDistribution::lognormal_distribution());

            bench_rendezvous(nodes, items, KeyDistribution::uniform_distribution());
            bench_rendezvous(nodes, items, KeyDistribution::normal_distribution());
            bench_rendezvous(nodes, items, KeyDistribution::lognormal_distribution());
        }
    }
}
