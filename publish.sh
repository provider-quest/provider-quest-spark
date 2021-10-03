#! /bin/bash

set -e

./publish-power-latest.sh
./publish-power-daily.sh
./publish-power-multiday.sh
./publish-power-by-region.sh
./publish-info-subset.sh
./publish-asks-subset.sh
./publish-deals.sh
./publish-deals-multiday.sh
./publish-dht-addrs.sh
./publish-ips-geolite2.sh
./publish-ips-baidu.sh
./publish-regions-locations.sh
