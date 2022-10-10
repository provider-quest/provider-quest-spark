#! /bin/bash

set -eo pipefail

mkdir -p $WORK_DIR/tmp
stdbuf -o0 ./scan-multiaddrs-ips-worker.sh 2>&1 | tee -a $WORK_DIR/tmp/scan.log
