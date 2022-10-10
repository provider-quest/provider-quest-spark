#! /bin/bash

set -eo pipefail

export TZ=UTC

echo '>> IPs => GeoLite2 lookups'
timeout 30m node scan-ips-geolite2.js

echo '>> IPs => Baidu lookups'
timeout 30m node scan-ips-baidu.js

