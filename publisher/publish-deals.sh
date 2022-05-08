#! /bin/bash

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

mkdir -p $WORK_DIR/dist/deals

# Latest deal data
(
	cd ..
       	make -f Makefile.deals
)

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

