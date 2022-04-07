#! /bin/bash

set -e

export TZ=UTC

echo '>> Synthetic Locations'
timeout 120m node scan-synthetic-locations.js

