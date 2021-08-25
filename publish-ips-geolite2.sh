#! /bin/sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

# Latest GeoIP lookups
mkdir -p dist/geoip-lookups

if [ -f ../work/output/ips_geolite2/json_latest/_SUCCESS ] ; then
  PART=$(ls ../work/output/ips_geolite2/json_latest/part*.json | head -1)
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
  }" > dist/geoip-lookups/ips-geolite2-latest.json
fi

(cd dist/geoip-lookups; head ips-geolite2-latest.json; hub bucket push -y)
