#! /bin/bash

export TZ=UTC
while true; do

  echo '>> Miner Power'
  timeout 30m node scan-miner-power.js
  timeout 30m node scan-miner-power.js --newest-not-recent
  timeout 30m node scan-miner-power.js --all-not-recent

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> Miner Info'
  timeout 30m node scan-miner-info.js

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> Asks'
  timeout 5m node scan-asks.js
  timeout 4m node scan-asks.js --no-recents

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> IPs => GeoLite2 lookups'
  timeout 30m node scan-ips-geolite2.js

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> IPs => Baidu lookups'
  timeout 30m node scan-ips-baidu.js

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> Deals (1 of 3)'
  timeout 30m node scan-deals.js
  echo '>> (finished) Deals (1 of 3)'

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> DHT Addrs (No fail)'
  timeout 30m node scan-dht-addrs.js
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
  timeout 30m node scan-deals.js
  echo '>> (finished) Deals (2 of 3)'

  echo
  echo 'Sleeping 60s' $(TZ=America/Vancouver date)
  echo
  sleep 60

  echo '>> DHT Addrs (No recents)'
  timeout 30m node scan-dht-addrs.js --no-recents
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
  timeout 30m node scan-deals.js
  echo '>> (finished) Deals (3 of 3)'

  echo
  echo 'Sleeping 30m' $(TZ=America/Vancouver date)
  echo
  sleep $((20 * 60))

  echo '>> Publishing everything'
  ./publish.sh

  echo
  echo 'Sleeping 15s' $(TZ=America/Vancouver date)
  echo
  sleep 15

  echo '>> Multiaddrs + IPs'
  timeout 30m node scan-multiaddrs-ips.js

  echo
  echo 'Sleeping 15s' $(TZ=America/Vancouver date)
  echo
  sleep 15

  echo '>> Regions and Locations'
  timeout 1m node scan-miner-regions-locations.js
  timeout 1m node scan-provider-country-state-province.js

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
