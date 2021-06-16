#! /bin/bash

mkdir -p input-staging/miner-power \
  input-staging/miner-info \
  input-staging/asks
COUNT=0
#for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -150`; do
for f in `ls estuary-archive/asks/asks-*.json | sort | tail -100`; do
#for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -10`; do
  echo $COUNT $f
  if [ $((COUNT % 1)) = "0" ]; then
    cp -v $f input-staging/asks
  fi
  echo $((COUNT++)) > /dev/null
  sleep 120
done

