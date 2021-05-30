#! /bin/bash

for f in `ls -d output/json_avg_power_hourly/miner\=f0*`; do
  miner=$(echo $f | sed 's,^.*miner=,,')
  echo $miner
  cat $(ls $f/part*.json) | jq -s '[.[] | { start: .window.start, qualityAdjPower: .["avg(qualityAdjPower)"] }] | sort_by(.start)'
done
