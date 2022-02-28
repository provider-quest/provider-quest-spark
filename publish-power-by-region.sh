#! /bin/bash

if [ ! -f PUBLISH ]; then
	echo Skipping publishing, PUBLISH file is missing
	exit
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest power
mkdir -p dist/miner-power-daily-average-latest

if [ -f ../work/output/miner_power/by_miner_region/sum_avg_daily/json/_SUCCESS ] ; then
  PART=$(ls ../work/output/miner_power/by_miner_region/sum_avg_daily/json/part*.json | head -1)

  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    rows: .
  }" > dist/miner-power-daily-average-latest/miner-power-by-region.json

fi

if [ -f ../work/output/miner_power/by_synthetic_region/sum_avg_daily/json/_SUCCESS ] ; then
  PART=$(ls ../work/output/miner_power/by_synthetic_region/sum_avg_daily/json/part*.json | head -1)

  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    rows: .
  }" > dist/miner-power-daily-average-latest/provider-power-by-synthetic-region.json

fi

(
  cd dist/miner-power-daily-average-latest
  head miner-power-by-region.json
  head provider-power-by-synthetic-region.json
  hub bucket push -y
)

