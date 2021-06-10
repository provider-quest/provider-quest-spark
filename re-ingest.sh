#! /bin/bash

mkdir -p input/miner-power input/miner-info
COUNT=0
#for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -150`; do
for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -300`; do
#for f in `ls estuary-archive/miner-power/power-*.json | sort | tail -10`; do
  echo $((COUNT++)) $f
  cp $f input/miner-power
  sleep 5
done

