#! /bin/bash

export TZ=UTC
while true; do

  echo '>> Miner Power'
  node scan-miner-power.js
  node scan-miner-power.js --newest-not-recent
  node scan-miner-power.js --all-not-recent

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
  node scan-asks.js --no-recents

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
  node scan-dht-addrs.sh
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

  echo '>> DHT Addrs (No recents)'
  node scan-dht-addrs.sh --no-recents
  echo '>> (finished) DHT Addrs (No recents)'

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
  echo 'Sleeping 15s' $(TZ=America/Vancouver date)
  echo
  sleep 15

  echo '>> Multiaddrs + IPs'
  node scan-multiaddrs-ips.js

  echo
  echo 'Sleeping 5m' $(TZ=America/Vancouver date)
  echo
  sleep $((5 * 60))

  echo '>> Publishing Multiaddrs + IPs'
  ./publish-multiaddrs-ips.sh

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60


done
