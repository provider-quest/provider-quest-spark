#! /bin/sh

set -e

./publish-power-latest.sh
./publish-power-daily.sh
./publish-power-multiday.sh
./publish-info-subset.sh
./publish-asks-subset.sh
./publish-deals.sh
./publish-deals-multiday.sh
./publish-dht-addrs.sh
