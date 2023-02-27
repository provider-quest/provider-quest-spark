#! /bin/bash

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

mkdir -p $WORK_DIR/dist/deals

# Latest deal data
(
	cd ..
       	make -f Makefile.deals
)

