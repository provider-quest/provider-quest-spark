#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

TARGET=$WORK_DIR/dist/dht-addrs-latest
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
        cd $TARGET
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest DHT addrs
if [ -f $OUTPUT_DHT_ADDRS_DIR/json_latest_subset/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_DHT_ADDRS_DIR/json_latest_subset/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    miners: map({ \
      key: .miner, value: { \
        epoch: .[\"last(epoch)\"], \
        timestamp: .[\"last(timestamp)\"], \
        collectedFrom: .[\"last(collectedFrom)\"], \
        peerId: .[\"last(peerId)\"], \
        multiaddrs: .[\"last(multiaddrs)\"], \
        dnsLookups: \
          (if .[\"last(dnsLookups)\"] then \
            .[\"last(dnsLookups)\"] | to_entries | map(.value = [.value | map(fromjson)][0]) | from_entries \
          else \
            null \
          end) \
      } | to_entries | [(.[] | select(.value != null))] | from_entries \
    }) | from_entries \
  }" > $TMP/dht-addrs-latest.json
fi

# Daily counts
LASTDAILY=$(ls -d $OUTPUT_DHT_ADDRS_DIR/json_counts_daily/date\=* | sort | tail -1)
echo $LASTDAILY
DATEDAILY=$(echo $LASTDAILY | sed 's,^.*date=,,')
echo $DATEDAILY
cat $OUTPUT_DHT_ADDRS_DIR/json_counts_daily/date\=$DATEDAILY/part-*.json |
  jq -s "{ \
    date: \"$DATEDAILY\", \
    miners: map({ \
      key: .miner, \
      value: .count \
    }) | from_entries \
  }" > $TMP/dht-addrs-counts-daily.json

# Multiday counts
LASTMULTIDAY=$(ls -d $OUTPUT_DHT_ADDRS_DIR/json_counts_multiday/window\=* | sort | tail -1)
echo $LASTMULTIDAY
DATEMULTIDAY=$(echo $LASTMULTIDAY | sed 's,^.*window=%7B\([^ ]*\).*,\1,')
echo $DATEMULTIDAY
cat $LASTMULTIDAY/part-*.json |
  jq -s "{ \
    date: \"$DATEMULTIDAY\", \
    intervalDays: 7, \
    miners: map({ \
      key: .miner, \
      value: .count \
    }) | from_entries \
  }" > $TMP/dht-addrs-counts-multiday.json

(
  set -e

  cd $TARGET

  mv $TMP/dht-addrs-latest.json .
  echo "dht-addrs-latest.json:"
  head dht-addrs-latest.json

  mv $TMP/dht-addrs-counts-daily.json .
  echo "dht-addrs-counts-daily.json:"
  head dht-addrs-counts-daily.json

  mv $TMP/dht-addrs-counts-multiday.json .
  echo "dht-addrs-counts-multiday.json:"
  head dht-addrs-counts-multiday.json
)

