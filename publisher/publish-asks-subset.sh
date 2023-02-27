#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

TARGET=$WORK_DIR/dist/asks-subset-latest
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
        cd $TARGET
fi


IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

if [ -f $OUTPUT_ASKS_DIR/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_ASKS_DIR/json_latest_subset/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    miners: map({ \
      key: .miner, \
      value: { \
        epoch: .[\"last(epoch)\"], \
        timestamp: .[\"last(timestamp)\"], \
        price: .[\"last(price)\"], \
        verifiedPrice: .[\"last(verifiedPrice)\"], \
        priceDouble: .[\"last(priceDouble)\"], \
        verifiedPriceDouble: .[\"last(verifiedPriceDouble)\"], \
        minPieceSize: .[\"last(minPieceSize)\"], \
        maxPieceSize: .[\"last(maxPieceSize)\"], \
        askTimestamp: .[\"last(askTimestamp)\"], \
        expiry: .[\"last(expiry)\"], \
        seqNo: .[\"last(seqNo)\"], \
        error: .[\"last(error)\"] \
      } \
    }) | from_entries \
  }" > $TMP/asks-subset-latest.json
fi

(
  set -e

  cd $TARGET

  mv $TMP/asks-subset-latest.json .
  echo "asks-subset-latest.json:"
  head asks-subset-latest.json
)

