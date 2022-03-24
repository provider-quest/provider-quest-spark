#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

./setup-textile.sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString().slice(0, 10))')

TARGET=$WORK_DIR/dist/miner-power-daily-average-latest
if [ ! -d $TARGET ]; then
	mkdir -p $TARGET
	cd $TARGET
	hub bucket init \
		--thread $TEXTILE_BUCKET_THREAD \
		--key $BUCKET_MINER_POWER_DAILY_AVERAGE_LATEST_KEY
fi


if [ -f $OUTPUT_POWER_REGIONS_DIR/sum_avg_daily/json/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_POWER_REGIONS_DIR/sum_avg_daily/json/part*.json | head -1)

  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    rows: .
  }" > $TMP/miner-power-by-region.json

fi

#if [ -f ../work/output/miner_power/by_synthetic_region/sum_avg_daily/json/_SUCCESS ] ; then
#  PART=$(ls ../work/output/miner_power/by_synthetic_region/sum_avg_daily/json/part*.json | head -1)
#
#  cat $PART | jq -s "{ \
#    date: \"$DATE\", \
#    rows: .
#  }" > tmp/provider-power-by-synthetic-region.json
#
#fi

cd $TARGET
hub bucket pull -y
mv $TMP/miner-power-by-region.json .
head miner-power-by-region.json
hub bucket push -y


#cd dist/miner-power-daily-average-latest
#hub bucket pull -y
#mv ../../tmp/miner-power-by-region.json .
#mv ../../tmp/provider-power-by-synthetic-region.json .
#head miner-power-by-region.json
#head provider-power-by-synthetic-region.json
#hub bucket push -y

