#! /bin/bash

set -eo pipefail

export TZ=UTC

timeout 30m node scan-deals.js

