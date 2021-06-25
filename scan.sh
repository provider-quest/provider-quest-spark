#! /bin/bash

export TZ=UTC
while true; do
  echo '>> Miner Power'
  node scan-miner-power.js
  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60
  echo '>> Miner Info'
  node scan-miner-info.js
  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60
  echo '>> Asks'
  node scan-asks.js
  echo
  echo 'Sleeping 2h' $(TZ=America/Vancouver date)
  echo
  sleep 7200
done
