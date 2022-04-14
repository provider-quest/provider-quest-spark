#! /bin/bash

if [ ! -f PUBLISH ]; then
	echo Skipping publishing, PUBLISH file is missing
	exit
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest deal data
mkdir -p dist/deals/named-clients
make -f Makefile.deals

#(cd dist/deals; hub bucket push -y; hub bucket pull -y)
#(
#  cd dist/deals
#  hub bucket push -y
#  if [ $? ]; then
    #echo Error, trying to pull then push
    #hub bucket pull -y
    #hub bucket push -y
#  fi
#)

