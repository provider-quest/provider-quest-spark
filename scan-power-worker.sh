#! /bin/bash

export TZ=UTC

echo '>> Miner Power'
timeout 30m node scan-miner-power.js
timeout 30m node scan-miner-power.js --newest-not-recent
timeout 30m node scan-miner-power.js --all-not-recent

