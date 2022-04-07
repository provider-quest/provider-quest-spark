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

# Latest miner-regions

LAST_REGIONS=$(cd $INPUT_MINER_REGIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_REGIONS

if [ -f $INPUT_MINER_REGIONS_DIR/$LAST_REGIONS/miner-regions-*.json ] ; then
  JSON=$INPUT_MINER_REGIONS_DIR/$LAST_REGIONS/miner-regions-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_REGIONS,
    minerRegions: . \
  }" > $TMP/miner-regions-latest.json
fi

exit

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

# Latest region-hierarchy

LAST_HIERARCHY=$(cd input/region-hierarchy; ls | sort -n | tail -1)
#echo $LAST_HIERARCHY

if [ -f input/region-hierarchy/$LAST_HIERARCHY/region-hierarchy-*.json ] ; then
  JSON=input/region-hierarchy/$LAST_HIERARCHY/region-hierarchy-*.json
  cat $JSON | jq "{ \
    date: \"$DATE\", \
    epoch: $LAST_HIERARCHY,
    regionHierarchy: . \
  }" > dist/geoip-lookups/region-hierarchy-latest.json
fi


# Latest provider-country-state-province

LAST_CSP_REGIONS=$(cd input/provider-country-state-province; ls | sort -n | tail -1)
#echo $LAST_CSP_REGIONS

if [ -f input/provider-country-state-province/$LAST_CSP_REGIONS/provider-country-state-province-*.json ] ; then
  JSON=input/provider-country-state-province/$LAST_CSP_REGIONS/provider-country-state-province-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_CSP_REGIONS,
    minerRegions: . \
  }" > dist/geoip-lookups/provider-country-state-province-latest.json
fi

# Latest provider-country-state-province-locations

LAST_CSP_LOCATIONS=$(cd input/provider-country-state-province-locations; ls | sort -n | tail -1)
#echo $LAST_CSP_LOCATIONS

if [ -f input/provider-country-state-province-locations/$LAST_CSP_LOCATIONS/provider-country-state-province-locations-*.json ] ; then
  JSON=input/provider-country-state-province-locations/$LAST_CSP_LOCATIONS/provider-country-state-province-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_CSP_LOCATIONS,
    minerLocations: . \
  }" > dist/geoip-lookups/provider-country-state-province-locations-latest.json
fi

# Latest country-state-province-hierarchy

LAST_CSP_HIERARCHY=$(cd input/country-state-province-hierarchy; ls | sort -n | tail -1)
#echo $LAST_CSP_HIERARCHY

if [ -f input/country-state-province-hierarchy/$LAST_CSP_HIERARCHY/country-state-province-hierarchy-*.json ] ; then
  JSON=input/country-state-province-hierarchy/$LAST_CSP_HIERARCHY/country-state-province-hierarchy-*.json
  cat $JSON | jq "{ \
    date: \"$DATE\", \
    epoch: $LAST_CSP_HIERARCHY,
    regionHierarchy: . \
  }" > dist/geoip-lookups/country-state-province-hierarchy-latest.json
fi

# Latest synthetic-regions

LAST_SYNTHETIC_REGIONS=$(cd input/synthetic-regions; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_REGIONS

if [ -f input/synthetic-regions/$LAST_SYNTHETIC_REGIONS/synthetic-provider-regions-*.json ] ; then
  JSON=input/synthetic-regions/$LAST_SYNTHETIC_REGIONS/synthetic-provider-regions-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_REGIONS,
    regions: . \
  }" > dist/geoip-lookups/synthetic-regions-latest.json
fi

LAST_SYNTHETIC_CSP_REGIONS=$(cd input/synthetic-regions; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_CSP_REGIONS

if [ -f input/synthetic-regions/$LAST_SYNTHETIC_CSP_REGIONS/synthetic-provider-country-state-province-*.json ] ; then
  JSON=input/synthetic-regions/$LAST_SYNTHETIC_CSP_REGIONS/synthetic-provider-country-state-province-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_CSP_REGIONS,
    regions: . \
  }" > dist/geoip-lookups/synthetic-country-state-province-latest.json
fi

LAST_SYNTHETIC_LOCATIONS=$(cd input/synthetic-locations; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_LOCATIONS

if [ -f input/synthetic-locations/$LAST_SYNTHETIC_LOCATIONS/synthetic-provider-locations-*.json ] ; then
  JSON=input/synthetic-locations/$LAST_SYNTHETIC_LOCATIONS/synthetic-provider-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_LOCATIONS,
    providerLocations: . \
  }" > dist/geoip-lookups/synthetic-locations-latest.json
fi

LAST_SYNTHETIC_CSP_LOCATIONS=$(cd input/synthetic-locations; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_CSP_LOCATIONS

if [ -f input/synthetic-locations/$LAST_SYNTHETIC_CSP_LOCATIONS/synthetic-provider-country-state-province-locations-*.json ] ; then
  JSON=input/synthetic-locations/$LAST_SYNTHETIC_CSP_LOCATIONS/synthetic-provider-country-state-province-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_CSP_LOCATIONS,
    providerLocations: . \
  }" > dist/geoip-lookups/synthetic-country-state-province-locations-latest.json
fi

(
  cd dist/geoip-lookups;
  echo "miner-regions-latest.json:"
  head miner-regions-latest.json
  echo "miner-locations-latest.json:"
  head miner-locations-latest.json
  echo "region-hierarchy-latest.json:"
  head region-hierarchy-latest.json
  echo "provider-country-state-province-latest.json:"
  head provider-country-state-province-latest.json
  echo "provider-country-state-province-locations-latest.json:"
  head provider-country-state-province-locations-latest.json
  echo "country-state-province-hierarchy-latest.json:"
  head country-state-province-hierarchy-latest.json
  echo "synthetic-regions-latest.json:"
  head synthetic-regions-latest.json
  echo "synthetic-country-state-province-latest.json:"
  head synthetic-country-state-province-latest.json
  echo "synthetic-locations-latest.json:"
  head synthetic-locations-latest.json
  echo "synthetic-country-state-province-locations-latest.json:"
  head synthetic-country-state-province-locations-latest.json
  hub bucket push -y
)
