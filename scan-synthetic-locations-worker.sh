#! /bin/bash

export TZ=UTC

echo '>> Synthetic Locations'
timeout 30m node scan-synthetic-locations.js

