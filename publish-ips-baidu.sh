#! /bin/bash

if [ ! -f PUBLISH ]; then
	echo Skipping publishing, PUBLISH file is missing
	exit
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest GeoIP lookups
mkdir -p dist/geoip-lookups

if [ -f ../work/output/ips_baidu/json_latest/_SUCCESS ] ; then
  PART=$(ls ../work/output/ips_baidu/json_latest/part*.json | head -1)
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
  }" > dist/geoip-lookups/ips-baidu-latest.json
fi

(cd dist/geoip-lookups; head ips-baidu-latest.json; hub bucket push -y)
