#! /bin/sh

set -e

# Latest daily power average
mkdir -p dist/miner-power-daily-average-latest
LAST=$(ls -d output/json_avg_power_daily/date\=* | sort | tail -1)
echo $LAST
DATE=$(echo $LAST | sed 's,^.*date=,,')
echo $DATE
COUNT=0
(for m in $(ls -d $LAST/miner\=f0*); do
  MINER=$(echo $m | sed 's,^.*miner=,,')
  echo Daily $((++COUNT)) $MINER 1>&2
  # {"window":{"start":"2021-05-30T00:00:00.000Z","end":"2021-05-31T00:00:00.000Z"},"avg(rawBytePower)":0.0,"avg(qualityAdjPower)":0.0}
  cat $(ls $m/*.json) | head -1 | jq "{ miner: \"$MINER\", rawBytePower: .[\"avg(rawBytePower)\"], qualityAdjPower: .[\"avg(qualityAdjPower)\"] }"
done) | jq -s "{ date: \"$DATE\", miners: map({ key: .miner, value: { qualityAdjPower: .qualityAdjPower, rawBytePower: .rawBytePower } }) | from_entries }" > dist/miner-power-daily-average-latest/miner-power-daily-average-latest.json
(cd dist/miner-power-daily-average-latest; head miner-power-daily-average-latest.json; hub bucket push -y)

./publish-multiday.sh
