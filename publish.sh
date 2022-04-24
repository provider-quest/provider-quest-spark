#! /bin/bash

#set -e

if [ ! -f PUBLISH ]; then
	echo Skipping publishing, PUBLISH file is missing
	exit
fi

timeout 15m ./publish-asks-subset.sh
timeout 15m ./publish-deals.sh
timeout 15m ./publish-deals-multiday.sh
