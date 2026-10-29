#!/usr/bin/bash
# (Re)starts the Toolforge web service from the image ./build.sh built, running
# the `web` entry of the Procfile. --mount=all because config.json and the
# database credentials are in the tool's home directory.
toolforge webservice buildservice restart --mount=all --mem 4G --cpu 2
