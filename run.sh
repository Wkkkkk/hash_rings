#!/bin/bash
echo "Hello World"

#rm -rf ./scr/script/

for ch in "consistent_hashing" "jump_hashing" "carp_hashing" "mpc_hashing" "maglev_hashing" "rendezvous_hashing" "weighted_rendezvous_hashing"
do
echo $ch
mkdir -p ./src/scripts/$ch
done

cargo build
cargo run