#! /bin/bash

stdbuf -o0 ./scan-worker.sh 2>&1 | tee -a ~/tmp/scan.log
