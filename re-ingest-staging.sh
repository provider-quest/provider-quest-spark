#! /bin/bash

mkdir -p input-staging/miner-power \
  input-staging/miner-info \
  input-staging/asks \
  input-staging/deals
COUNT=0
FILES=$(node sorted-archive-json-files.js $((1 * 24 * 60 * 2)))
for f in $FILES; do
  echo $COUNT $f
  DEST=$(echo $f | sed 's,estuary-archive/,input-staging/,')
  if [ $((COUNT % 1)) = "0" ]; then
    cp -v $f $DEST
  fi
  echo $((COUNT++)) > /dev/null
  sleep 1
done

