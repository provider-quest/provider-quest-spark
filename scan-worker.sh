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
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> Deals (1 of 3)'
  node scan-deals.js
  echo '>> (finished) Deals (1 of 3)'

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> DHT Addrs (No fail)'
  node scan-dht-peers.sh
  echo '>> (finished) DHT Addrs (No fail)'

  echo
  echo 'Sleeping 5m' $(TZ=America/Vancouver date)
  echo
  sleep $((5 * 60))

  echo '>> Publishing deals'
  ./publish-deals.sh

  echo
  echo 'Sleeping 15s' $(TZ=America/Vancouver date)
  echo
  sleep 15

  echo '>> Deals (2 of 3)'
  node scan-deals.js
  echo '>> (finished) Deals (2 of 3)'

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> DHT Addrs (Fail only)'
  node scan-dht-peers.sh --fail-only
  echo '>> (finished) DHT Addrs (Fail only)'

  echo
  echo 'Sleeping 15m' $(TZ=America/Vancouver date)
  echo
  sleep $((15 * 60))

  echo '>> Publishing deals'
  ./publish-deals.sh

  echo
  echo 'Sleeping 15s' $(TZ=America/Vancouver date)
  echo
  sleep 15

  echo '>> Deals (3 of 3)'
  node scan-deals.js
  echo '>> (finished) Deals (3 of 3)'

  echo
  echo 'Sleeping 20m' $(TZ=America/Vancouver date)
  echo
  sleep $((20 * 60))

  echo '>> Publishing everything'
  ./publish.sh

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60


done
