#! /bin/bash

mkdir -p input/miner-power \
  input/miner-info \
  input/asks \
  input/deals \
  input/dht-addrs
COUNT=0
FILES=$(node sorted-archive-json-files.js $((8 * 24 * 60 * 2)))
for f in $FILES; do
  echo $COUNT $f
  DEST=$(echo $f | sed 's,estuary-archive/,input/,')
  if [ $((COUNT % 1)) = "0" ]; then
    cp -v $f $DEST
  fi
  echo $((COUNT++)) > /dev/null
  sleep 1
done

