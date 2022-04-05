#! /bin/bash

stdbuf -o0 ./scan-synthetic-locations-worker.sh 2>&1 | tee -a $WORK_DIR/tmp/scan.log
