#!/bin/bash
# Get latest code
git pull

# Update
cargo update

# Build new server binary
cargo build --release

# Get restart code from config file
code=`jq -r '.["restart-code"]' config.json`

# Build restart URL
url="http://127.0.0.1/?restart=$code"

# Restart server
curl -s -o /dev/null $url
sleep 1
service ./target/release/petscan_rs restart

