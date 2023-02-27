#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

TARGET=$WORK_DIR/dist/miner-power-daily-average-latest
if [ ! -d $TARGET ]; then
	mkdir -p $TARGET
	cd $TARGET
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
        maxRawBytePower: .[\"max(rawBytePower)\"], \
        maxQualityAdjPower: .[\"max(qualityAdjPower)\"], \
      } | to_entries | [(.[] | select(.value != null))] | from_entries \
    }) | from_entries \
  }" > $TMP/miner-power-latest.json

fi

cd $TARGET
mv $TMP/miner-power-latest.json .
echo "miner-power-latest.json:"
head miner-power-latest.json

