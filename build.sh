#!/usr/bin/bash
# Builds the Toolforge image from the pushed code on GitHub (not the local
# checkout). Run this as the petscan tool on Toolforge, then ./restart.sh.
toolforge build start https://github.com/magnusmanske/petscan_rs/
