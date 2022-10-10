#! /bin/bash

set -eo pipefail

export TZ=UTC

echo '>> Regions and Locations'
timeout 1m node scan-miner-regions-locations.js
timeout 1m node scan-provider-country-state-province.js

