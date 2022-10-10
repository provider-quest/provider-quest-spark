#! /bin/bash

set -e

export TZ=UTC

echo '>> Synthetic Locations'
timeout 240m node scan-synthetic-locations.js

