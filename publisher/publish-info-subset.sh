#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

./setup-textile.sh

TARGET=$WORK_DIR/dist/miner-info-subset-latest
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
        cd $TARGET
        hub bucket init \
                --thread $TEXTILE_BUCKET_THREAD \
                --key $BUCKET_MINER_INFO_SUBSET_LATEST_KEY
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

if [ -f $OUTPUT_MINER_INFO_DIR/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_MINER_INFO_DIR/json_latest_subset/part*.json | head -1)
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
  }" > $TMP/miner-info-subset-latest.json
fi

(
  set -e

  cd $TARGET
  hub bucket pull

  mv $TMP/miner-info-subset-latest.json .
  echo "miner-info-subset-latest.json:"
  head miner-info-subset-latest.json

  hub bucket push -y
)

