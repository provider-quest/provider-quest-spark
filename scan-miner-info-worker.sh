#! /bin/bash

set -eo pipefail

export TZ=UTC

echo '>> Miner Info'
timeout 30m node scan-miner-info.js

