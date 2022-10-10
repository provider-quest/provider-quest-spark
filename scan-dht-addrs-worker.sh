#! /bin/bash

set -eo pipefail

export TZ=UTC

echo '>> DHT Addrs (No fail)'
timeout 30m node scan-dht-addrs.js
echo '>> (finished) DHT Addrs (No fail)'

echo '>> DHT Addrs (No recents)'
timeout 30m node scan-dht-addrs.js --no-recents
echo '>> (finished) DHT Addrs (No recents)'


