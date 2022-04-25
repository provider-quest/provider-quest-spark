#! /bin/bash

set -e

export TZ=UTC

echo '>> Multiaddrs + IPs'
timeout 30m node scan-multiaddrs-ips.js

