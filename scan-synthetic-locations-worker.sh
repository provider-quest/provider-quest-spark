#! /bin/bash

set -eo pipefail

export TZ=UTC

echo '>> Synthetic Locations'
timeout 240m node scan-synthetic-locations.js

