#! /bin/sh

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

mkdir -p dist/geoip-lookups

# Latest miner-regions

LAST_REGIONS=$(cd input/miner-regions; ls | sort -n | tail -1)
#echo $LAST_REGIONS

if [ -f input/miner-regions/$LAST_REGIONS/miner-regions-*.json ] ; then
  JSON=input/miner-regions/$LAST_REGIONS/miner-regions-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_REGIONS,
    minerRegions: . \
  }" > dist/geoip-lookups/miner-regions-latest.json
fi

# Latest miner-locations

LAST_LOCATIONS=$(cd input/miner-locations; ls | sort -n | tail -1)
#echo $LAST_LOCATIONS

if [ -f input/miner-locations/$LAST_LOCATIONS/miner-locations-*.json ] ; then
  JSON=input/miner-locations/$LAST_LOCATIONS/miner-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_LOCATIONS,
    minerLocations: . \
  }" > dist/geoip-lookups/miner-locations-latest.json
fi

(
  cd dist/geoip-lookups;
  echo "miner-regions-latest.json:"
  head miner-regions-latest.json
  echo "miner-locations-latest.json:"
  head miner-locations-latest.json
  hub bucket push -y
)
