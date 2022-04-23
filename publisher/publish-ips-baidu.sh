#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

./setup-textile.sh

TARGET=$WORK_DIR/dist/geoip-lookups
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
        cd $TARGET
        hub bucket init \
                --thread $TEXTILE_BUCKET_THREAD \
                --key $BUCKET_GEOIP_LOOKUPS_KEY
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest GeoIP lookups

if [ -f $OUTPUT_IPS_BAIDU_DIR/json_latest/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_IPS_BAIDU_DIR/json_latest/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    ipsBaidu: map({ \
      key: .ip, value: { \
        epoch: .[\"last(epoch)\"], \
        timestamp: .[\"last(timestamp)\"], \
        city: .[\"last(city)\"], \
        long: .[\"last(long)\"], \
        lat: .[\"last(lat)\"], \
        baidu: .[\"last(baidu)\"] | fromjson \
      } | to_entries | [(.[] | select(.value != null))] | from_entries \
    }) | from_entries \
  }" > $TMP/ips-baidu-latest.json
fi

(
  set -e

  cd $TARGET
  hub bucket pull

  mv $TMP/ips-baidu-latest.json .
  echo "ips-baidu-latest.json:"
  head ips-baidu-latest.json

  hub bucket push -y
)
