#! /bin/bash

mkdir -p input-staging
COUNT=0
#for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -150`; do
for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -300`; do
#for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -10`; do
  echo $((COUNT++)) $f
  if [ $((COUNT % 6)) = "1" ]; then
    cp -v $f input-staging
  fi
  sleep 1
done

