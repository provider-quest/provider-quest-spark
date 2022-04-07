#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

./setup-textile.sh

TARGET=$WORK_DIR/dist/miner-power-daily-average-latest
if [ ! -d $TARGET ]; then
	mkdir -p $TARGET
	cd $TARGET
	hub bucket init \
		--thread $TEXTILE_BUCKET_THREAD \
		--key $BUCKET_MINER_POWER_DAILY_AVERAGE_LATEST_KEY
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

if [ -f $OUTPUT_POWER_DIR/json_latest/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_POWER_DIR/json_latest/part*.json | head -1)

  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    miners: [.[] | select(.miner != null)] | map({ \
      key: .miner, \
      value: { \
        epoch: .[\"last(epoch)\"], \
        timestamp: .[\"last(timestamp)\"], \
        rawBytePower: .[\"last(rawBytePower)\"], \
        qualityAdjPower: .[\"last(qualityAdjPower)\"], \
      } | to_entries | [(.[] | select(.value != null))] | from_entries \
    }) | from_entries \
  }" > $TMP/miner-power-latest.json

fi

cd $TARGET
hub bucket pull -y
mv $TMP/miner-power-latest.json .
echo "miner-power-latest.json:"
head miner-power-latest.json
hub bucket push -y

