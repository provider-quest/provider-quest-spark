#! /bin/bash

set -e

export TZ=UTC

timeout 5m node scan-asks.js
timeout 4m node scan-asks.js --no-recents

