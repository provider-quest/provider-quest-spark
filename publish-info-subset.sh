#! /bin/sh

IFS="$(printf '\n\t')"

# Latest multiday power average
mkdir -p dist/miner-info-subset-latest
if [ -f output/miner_info/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls output/miner_info/json_latest_subset/part*.json | head -1)
  cp -f $PART dist/miner-info-subset-latest/miner-info-subset-latest.json
fi
(cd dist/miner-info-subset-latest; head miner-info-subset-latest.json; hub bucket push -y)

