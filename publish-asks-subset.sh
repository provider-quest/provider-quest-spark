#! /bin/sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest multiday power average
mkdir -p dist/asks-subset-latest
if [ -f output/asks/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls output/asks/json_latest_subset/part*.json | head -1)
  cat $PART | jq -s "{ date: \"$DATE\", miners: map({ key: .miner, value: { price: .[\"last(price)\"], verifiedPrice: .[\"last(verifiedPrice)\"], priceDouble: .[\"last(priceDouble)\"], verifiedPriceDouble: .[\"last(verifiedPriceDouble)\"], minPieceSize: .[\"last(minPieceSize)\"], maxPieceSize: .[\"last(maxPieceSize)\"], askTimestamp: .[\"last(askTimestamp)\"], expiry: .[\"last(expiry)\"], seqNo: .[\"last(seqNo)\"], error: .[\"last(error)\"] } }) | from_entries }" > dist/asks-subset-latest/asks-subset-latest.json
fi
(cd dist/asks-subset-latest; head asks-subset-latest.json; hub bucket push -y)

