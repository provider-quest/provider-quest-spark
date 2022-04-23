#! /bin/bash

set -e

export TZ=UTC

echo '>> Miner Info'
timeout 30m node scan-miner-info.js

