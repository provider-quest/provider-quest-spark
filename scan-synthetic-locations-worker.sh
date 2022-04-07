#! /bin/bash

set -e

export TZ=UTC

echo '>> Synthetic Locations'
timeout 60m node scan-synthetic-locations.js

