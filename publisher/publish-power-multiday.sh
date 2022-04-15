#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

./setup-textile.sh

TARGET=$WORK_DIR/dist/miner-power-multiday-average-latest
if [ ! -d $TARGET ]; then
	mkdir -p $TARGET
	cd $TARGET
	#hub bucket init \
	#	--thread $TEXTILE_BUCKET_THREAD \
	#	--key $BUCKET_MINER_POWER_MULTIDAY_AVERAGE_LATEST_KEY
fi

IFS="$(printf '\n\t')"

# Latest multiday power average
LAST="$(ls -d $OUTPUT_POWER_DIR/json_avg_multiday/window\=* | sort | tail -1)"
echo $LAST
LAST_ESCAPE=$(echo $LAST | sed 's, ,\\ ,g')
DATE=$(echo $LAST | sed 's,^.*window=%7B\([^ ]*\).*,\1,')
echo $DATE

#for m in $LAST/miner\=f0*; do
#  echo $m
#done

COUNT=0
(for m in $LAST/miner\=f0*; do
  MINER=$(echo $m | sed 's,^.*miner=,,')
  echo Multiday $((++COUNT)) $MINER $m 1>&2
  # {"window":{"start":"2021-05-30T00:00:00.000Z","end":"2021-05-31T00:00:00.000Z"},"avg(rawBytePower)":0.0,"avg(qualityAdjPower)":0.0}
  cat $(ls $m/*.json) | head -1 | jq "{ \
    miner: \"$MINER\", \
    rawBytePower: .[\"avg(rawBytePower)\"], \
    qualityAdjPower: .[\"avg(qualityAdjPower)\"] \
  }"
done) | jq -s "{ \
  date: \"$DATE\", \
  intervalDays: 7, \
  miners: map({ \
    key: .miner, \
    value: { \
      qualityAdjPower: .qualityAdjPower, \
      rawBytePower: .rawBytePower \
    } \
  }) | from_entries }" > $TMP/miner-power-multiday-average-latest.json

cd $TARGET
#hub bucket pull -y
mv $TMP/miner-power-multiday-average-latest.json .
head miner-power-multiday-average-latest.json
#hub bucket push -y


