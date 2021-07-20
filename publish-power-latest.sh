#! /bin/sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest power
mkdir -p dist/miner-power-daily-average-latest

if [ -f output/miner_power/json_latest/_SUCCESS ] ; then
  PART=$(ls output/miner_power/json_latest/part*.json | head -1)

  #cat $PART | jq -s ".[] | select(.miner != null) | { \
  #  date: \"$DATE\", \
  #  miners: map({ \
  #    key: .miner, \
  #    value: { \
  #      epoch: .[\"last(epoch)\"], \
  #      timestamp: .[\"last(timestamp)\"], \
  #      rawBytePower: .[\"last(rawBytePower)\"], \
  #      qualityAdjPower: .[\"last(qualityAdjPower)\"], \
  #    } | to_entries | [(.[] | select(.value != null))] | from_entries \
  #  }) | from_entries \
  #}" > dist/miner-power-daily-average-latest/miner-power-latest.json

  #cat $PART | jq -s "{ \
  #  date: \"$DATE\", \
  #  miners: [.] | select(.miner != null) | .map({ \
  #    key: .miner, \
  #    value: true \
  #  }) \
  #}" > dist/miner-power-daily-average-latest/miner-power-latest.json

  #cat $PART | jq -s "{ \
  #  date: \"$DATE\", \
  #  miners: map(.miner) \
  #}"

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
  }" > dist/miner-power-daily-average-latest/miner-power-latest.json

fi

#(cd dist/miner-info-subset-latest; head miner-info-subset-latest.json; hub bucket push -y)

