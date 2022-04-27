#! /bin/bash

set -e

export TZ=UTC

timeout 30m node scan-deals.js

