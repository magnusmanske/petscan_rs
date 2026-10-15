#!/bin/bash
# Restarts PetScan whenever it exits. Each run is told how the previous one
# ended (PETSCAN_PREVIOUS_EXIT; 137 means SIGKILL, usually the memory limit),
# since a killed process cannot report that itself.
cd /home/magnus/petscan_rs/
status=""
for (( ; ; ))
do
sudo PETSCAN_PREVIOUS_EXIT=$status /home/magnus/petscan_rs/petscan_rs
status=$?
done
#sudo watch -n 0 /home/magnus/petscan_rs/target/release/petscan_rs
