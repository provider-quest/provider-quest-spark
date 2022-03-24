#! /bin/bash

if [ ! -f PUBLISH ]; then
	echo Skipping publishing, PUBLISH file is missing
	exit
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest power
mkdir -p dist/miner-power-daily-average-latest

#if [ -f ../work/output/miner_power/by_provider_country_state_province/sum_avg_daily/json/_SUCCESS ] ; then
#  PART=$(ls ../work/output/miner_power/by_provider_country_state_province/sum_avg_daily/json/part*.json | head -1)
#
#  cat $PART | jq -s "{ \
#    date: \"$DATE\", \
#    rows: .
#  }" > tmp/miner-power-by-country-state-province.json
#
#fi

if [ -f ../work/output/miner_power/by_synthetic_csp_region/sum_avg_daily/json/_SUCCESS ] ; then
  PART=$(ls ../work/output/miner_power/by_synthetic_csp_region/sum_avg_daily/json/part*.json | head -1)

  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    rows: .
  }" > tmp/provider-power-by-synthetic-csp-region.json

fi

cd dist/miner-power-daily-average-latest
hub bucket pull -y
#mv ../../tmp/miner-power-by-country-state-province.json .
mv ../../tmp/provider-power-by-synthetic-csp-region.json .
#head miner-power-by-country-state-province.json
head provider-power-by-synthetic-csp-region.json
hub bucket push -y

