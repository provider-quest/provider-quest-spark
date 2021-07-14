#! /bin/sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest multiday power average
mkdir -p dist/miner-info-subset-latest
if [ -f output/miner_info/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls output/miner_info/json_latest_subset/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    miners: map({ \
      key: .miner, value: { \
        sectorSize: .[\"last(sectorSize)\"], \
        peerId: .[\"last(peerId)\"], \
        multiaddrsDecoded: .[\"last(multiaddrsDecoded)\"] \
        dnsLookups: .[\"last(dnsLookups)\"] \
      } \
    }) | from_entries \
  }" > dist/miner-info-subset-latest/miner-info-subset-latest.json
fi
(cd dist/miner-info-subset-latest; head miner-info-subset-latest.json; hub bucket push -y)

