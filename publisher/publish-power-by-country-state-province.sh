#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

TARGET=$WORK_DIR/dist/miner-power-daily-average-latest
if [ ! -d $TARGET ]; then
	mkdir -p $TARGET
	cd $TARGET
fi

if [ -f $OUTPUT_POWER_CSP_REGIONS_DIR/sum_avg_daily/json/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_POWER_CSP_REGIONS_DIR/sum_avg_daily/json/part*.json | head -1)

  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    rows: .
  }" > $TMP/miner-power-by-country-state-province.json

fi

if [ -f $OUTPUT_POWER_SYNTHETIC_CSP_REGIONS_DIR/sum_avg_daily/json/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_POWER_SYNTHETIC_CSP_REGIONS_DIR/sum_avg_daily/json/part*.json | head -1)

  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    rows: .
  }" > $TMP/provider-power-by-synthetic-csp-region.json

fi

cd $TARGET

mv $TMP/miner-power-by-country-state-province.json .
echo miner-power-by-country-state-province.json:
head miner-power-by-country-state-province.json

mv $TMP/provider-power-by-synthetic-csp-region.json .
echo provider-power-by-synthetic-csp-region.json:
head provider-power-by-synthetic-csp-region.json


