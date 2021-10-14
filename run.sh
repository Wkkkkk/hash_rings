#!/bin/bash
echo "Hello World"

rm -rf ./src/scripts/

for ch in "consistent_hashing" "jump_hashing" "mpc_hashing" "maglev_hashing" "rendezvous_hashing"
do
echo $ch
mkdir -p ./src/scripts/$ch
echo -e "num_requests\tnum_servers\trequest_per_server\tdistribution\tthroughput\tstandard_error\tconfidence_interval" >> ./src/scripts/$ch.csv
done

cargo build
cargo run
