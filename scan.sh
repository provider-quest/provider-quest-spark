#! /bin/bash

set +x
set -e

export TZ=UTC
while true; do
  node scan-miner-power.js
  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60
  node scan-miner-info.js
  echo
  echo 'Sleeping 2h' $(TZ=America/Vancouver date)
  echo
  sleep 7200
done
