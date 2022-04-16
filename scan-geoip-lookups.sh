#! /bin/bash

set -e

mkdir -p $WORK_DIR/tmp

stdbuf -o0 ./scan-geoip-lookups-worker.sh 2>&1 | tee -a $WORK_DIR/tmp/scan.log
