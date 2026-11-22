#!/usr/bin/env bash
# Delopy and restart toolforge services
ssh login.toolforge.org 'become petscan bash -lc "cd ~/petscan_rs/ && git pull && ./build.sh && ./restart.sh && echo \"OK\""'
