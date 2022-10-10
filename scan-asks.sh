#! /bin/bash

set -eo pipefail

mkdir -p $ASKS_VOLUME/tmp
stdbuf -o0 ./scan-asks-worker.sh 2>&1 | tee -a $ASKS_VOLUME/tmp/scan.log
