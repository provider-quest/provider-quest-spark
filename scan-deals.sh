#! /bin/bash

set -e

mkdir -p $DEALS_VOLUME/tmp
stdbuf -o0 ./scan-deals-worker.sh 2>&1 | tee -a $DEALS_VOLUME/tmp/scan.log
