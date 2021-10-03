#! /bin/bash

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest multiday power average
mkdir -p dist/miner-info-subset-latest
if [ -f ../work/output/miner_info/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls ../work/output/miner_info/json_latest_subset/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    miners: map({ \
      key: .miner, value: { \
        epoch: .[\"last(epoch)\"], \
        timestamp: .[\"last(timestamp)\"], \
        sectorSize: .[\"last(sectorSize)\"], \
        peerId: .[\"last(peerId)\"], \
        multiaddrsDecoded: .[\"last(multiaddrsDecoded)\"], \
        dnsLookups: \
          (if .[\"last(dnsLookups)\"] then \
            .[\"last(dnsLookups)\"] | to_entries | map(.value = [.value | map(fromjson)][0]) | from_entries \
          else \
            null \
          end) \
      } | to_entries | [(.[] | select(.value != null))] | from_entries \
    }) | from_entries \
  }" > dist/miner-info-subset-latest/miner-info-subset-latest.json
fi
(cd dist/miner-info-subset-latest; head miner-info-subset-latest.json; hub bucket push -y)

