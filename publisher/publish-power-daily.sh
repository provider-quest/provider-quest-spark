#! /bin/bash

set -e
set +x

TARGET=$WORK_DIR/dist/miner-power-daily-average-latest
if [ ! -d $TARGET ]; then
	mkdir -p $TARGET
	cd $TARGET
	hub bucket init \
		--thread $TEXTILE_BUCKET_THREAD \
		--key $BUCKET_MINER_POWER_DAILY_AVERAGE_LATEST_KEY
fi

# Latest daily power average
LAST=$(ls -d $OUTPUT_POWER_DIR/json_avg_daily/date\=* | sort | tail -1)
echo $LAST
DATE=$(echo $LAST | sed 's,^.*date=,,')
echo $DATE
COUNT=0
(
  for m in $(ls -d $LAST/miner\=f0*); do
	  MINER=$(echo $m | sed 's,^.*miner=,,')
	  echo Daily $((++COUNT)) $MINER 1>&2
	  # {"window":{"start":"2021-05-30T00:00:00.000Z","end":"2021-05-31T00:00:00.000Z"},"avg(rawBytePower)":0.0,"avg(qualityAdjPower)":0.0}
	  cat $(ls $m/*.json) | head -1 | jq "{ miner: \"$MINER\", rawBytePower: .[\"avg(rawBytePower)\"], qualityAdjPower: .[\"avg(qualityAdjPower)\"] }"
  done) | jq -s "{ date: \"$DATE\", miners: map({ key: .miner, value: { qualityAdjPower: .qualityAdjPower, rawBytePower: .rawBytePower } }) | from_entries }" > $TARGET/miner-power-daily-average-latest.json

cd $TARGET
hub bucket pull -y
head miner-power-daily-average-latest.json
hub bucket push -y

