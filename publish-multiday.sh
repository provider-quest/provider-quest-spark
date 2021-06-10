#! /bin/sh

IFS="$(printf '\n\t')"

# Latest multiday power average
mkdir -p dist/miner-power-multiday-average-latest
LAST="$(ls -d output/miner_power/json_avg_multiday/window\=* | sort | tail -1)"
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
  cat $(ls $m/*.json) | head -1 | jq "{ miner: \"$MINER\", rawBytePower: .[\"avg(rawBytePower)\"], qualityAdjPower: .[\"avg(qualityAdjPower)\"] }"
done) | jq -s "{ date: \"$DATE\", miners: map({ key: .miner, value: { qualityAdjPower: .qualityAdjPower, rawBytePower: .rawBytePower } }) | from_entries }" > dist/miner-power-multiday-average-latest/miner-power-multiday-average-latest.json
(cd dist/miner-power-multiday-average-latest; head miner-power-multiday-average-latest.json; hub bucket push -y)

