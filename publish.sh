#! /bin/bash

#set -e

if [ ! -f PUBLISH ]; then
	echo Skipping publishing, PUBLISH file is missing
	exit
fi

timeout 15m ./publish-info-subset.sh
timeout 15m ./publish-asks-subset.sh
timeout 15m ./publish-deals.sh
timeout 15m ./publish-deals-multiday.sh
timeout 15m ./publish-dht-addrs.sh
timeout 15m ./publish-ips-geolite2.sh
timeout 15m ./publish-ips-baidu.sh
