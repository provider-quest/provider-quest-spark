#! /bin/bash

export TZ=UTC
while true; do

  echo '>> Asks'
  timeout 5m node scan-asks.js
  timeout 4m node scan-asks.js --no-recents

  echo
  echo 'Sleeping 60m' $(TZ=America/Vancouver date)
  echo
  sleep $((60 * 60))


done
