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

if [ -f $OUTPUT_IPS_GEOLITE2_DIR/json_latest/_SUCCESS ] ; then
  PART=$(ls $OUTPUT_IPS_GEOLITE2_DIR/json_latest/part*.json | head -1)
  cat $PART | jq -s "{ \
    date: \"$DATE\", \
    ipsGeoLite2: map({ \
      key: .ip, value: { \
        epoch: .[\"last(epoch)\"], \
        timestamp: .[\"last(timestamp)\"], \
        continent: .[\"last(continent)\"], \
        country: .[\"last(country)\"], \
        subdiv1: .[\"last(subdiv1)\"], \
        city: .[\"last(city)\"], \
        long: .[\"last(long)\"], \
        lat: .[\"last(lat)\"], \
        geolite2: .[\"last(geolite2)\"] | fromjson \
      } | to_entries | [(.[] | select(.value != null))] | from_entries \
    }) | from_entries \
  }" > $TMP/ips-geolite2-latest.json
fi

(
  set -e

  cd $TARGET
  hub bucket pull

  mv $TMP/ips-geolite2-latest.json .
  echo "ips-geolite2-latest.json:"
  head ips-geolite2-latest.json

  hub bucket push -y
)

