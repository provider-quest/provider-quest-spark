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

# Latest miner-locations

LAST_LOCATIONS=$(cd $INPUT_MINER_LOCATIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_LOCATIONS

if [ -f $INPUT_MINER_LOCATIONS_DIR/$LAST_LOCATIONS/miner-locations-*.json ] ; then
  JSON=$INPUT_MINER_LOCATIONS_DIR/$LAST_LOCATIONS/miner-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_LOCATIONS,
    minerLocations: . \
  }" > $TMP/miner-locations-latest.json
fi

# Latest region-hierarchy

LAST_HIERARCHY=$(cd $INPUT_REGION_HIERARCHY_DIR; ls | sort -n | tail -1)
#echo $LAST_HIERARCHY

if [ -f $INPUT_REGION_HIERARCHY_DIR/$LAST_HIERARCHY/region-hierarchy-*.json ] ; then
  JSON=$INPUT_REGION_HIERARCHY_DIR/$LAST_HIERARCHY/region-hierarchy-*.json
  cat $JSON | jq "{ \
    date: \"$DATE\", \
    epoch: $LAST_HIERARCHY,
    regionHierarchy: . \
  }" > $TMP/region-hierarchy-latest.json
fi

# Latest provider-country-state-province

LAST_CSP_REGIONS=$(cd $INPUT_MINER_CSP_REGIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_CSP_REGIONS

if [ -f $INPUT_MINER_CSP_REGIONS_DIR/$LAST_CSP_REGIONS/provider-country-state-province-*.json ] ; then
  JSON=$INPUT_MINER_CSP_REGIONS_DIR/$LAST_CSP_REGIONS/provider-country-state-province-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_CSP_REGIONS,
    minerRegions: . \
  }" > $TMP/provider-country-state-province-latest.json
fi

# Latest provider-country-state-province-locations

LAST_CSP_LOCATIONS=$(cd $INPUT_MINER_CSP_LOCATIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_CSP_LOCATIONS

if [ -f $INPUT_MINER_CSP_LOCATIONS_DIR/$LAST_CSP_LOCATIONS/provider-country-state-province-locations-*.json ] ; then
  JSON=$INPUT_MINER_CSP_LOCATIONS_DIR/$LAST_CSP_LOCATIONS/provider-country-state-province-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_CSP_LOCATIONS,
    minerLocations: . \
  }" > $TMP/provider-country-state-province-locations-latest.json
fi

# Latest country-state-province-hierarchy

LAST_CSP_HIERARCHY=$(cd $INPUT_CSP_REGION_HIERARCHY_DIR; ls | sort -n | tail -1)
#echo $LAST_CSP_HIERARCHY

if [ -f $INPUT_CSP_REGION_HIERARCHY_DIR/$LAST_CSP_HIERARCHY/country-state-province-hierarchy-*.json ] ; then
  JSON=$INPUT_CSP_REGION_HIERARCHY_DIR/$LAST_CSP_HIERARCHY/country-state-province-hierarchy-*.json
  cat $JSON | jq "{ \
    date: \"$DATE\", \
    epoch: $LAST_CSP_HIERARCHY,
    regionHierarchy: . \
  }" > $TMP/country-state-province-hierarchy-latest.json
fi

# Latest synthetic-regions

LAST_SYNTHETIC_REGIONS=$(cd $INPUT_SYNTHETIC_REGIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_REGIONS

if [ -f $INPUT_SYNTHETIC_REGIONS_DIR/$LAST_SYNTHETIC_REGIONS/synthetic-provider-regions-*.json ] ; then
  JSON=$INPUT_SYNTHETIC_REGIONS_DIR/$LAST_SYNTHETIC_REGIONS/synthetic-provider-regions-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_REGIONS,
    regions: . \
  }" > $TMP/synthetic-regions-latest.json
fi

LAST_SYNTHETIC_CSP_REGIONS=$(cd $INPUT_SYNTHETIC_REGIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_CSP_REGIONS

if [ -f $INPUT_SYNTHETIC_REGIONS_DIR/$LAST_SYNTHETIC_CSP_REGIONS/synthetic-provider-country-state-province-*.json ] ; then
  JSON=$INPUT_SYNTHETIC_REGIONS_DIR/$LAST_SYNTHETIC_CSP_REGIONS/synthetic-provider-country-state-province-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_CSP_REGIONS,
    regions: . \
  }" > $TMP/synthetic-country-state-province-latest.json
fi

LAST_SYNTHETIC_LOCATIONS=$(cd $INPUT_SYNTHETIC_LOCATIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_LOCATIONS

if [ -f $INPUT_SYNTHETIC_LOCATIONS_DIR/$LAST_SYNTHETIC_LOCATIONS/synthetic-provider-locations-*.json ] ; then
  JSON=$INPUT_SYNTHETIC_LOCATIONS_DIR/$LAST_SYNTHETIC_LOCATIONS/synthetic-provider-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_LOCATIONS,
    providerLocations: . \
  }" > $TMP/synthetic-locations-latest.json
fi

LAST_SYNTHETIC_CSP_LOCATIONS=$(cd $INPUT_SYNTHETIC_LOCATIONS_DIR; ls | sort -n | tail -1)
#echo $LAST_SYNTHETIC_CSP_LOCATIONS

if [ -f $INPUT_SYNTHETIC_LOCATIONS_DIR/$LAST_SYNTHETIC_CSP_LOCATIONS/synthetic-provider-country-state-province-locations-*.json ] ; then
  JSON=$INPUT_SYNTHETIC_LOCATIONS_DIR/$LAST_SYNTHETIC_CSP_LOCATIONS/synthetic-provider-country-state-province-locations-*.json
  cat $JSON | jq -s "{ \
    date: \"$DATE\", \
    epoch: $LAST_SYNTHETIC_CSP_LOCATIONS,
    providerLocations: . \
  }" > $TMP/synthetic-country-state-province-locations-latest.json
fi

(
  set -e

  cd $TARGET
  hub bucket pull

  mv $TMP/miner-regions-latest.json .
  echo "miner-regions-latest.json:"
  head miner-regions-latest.json

  mv $TMP/miner-locations-latest.json .
  echo "miner-locations-latest.json:"
  head miner-locations-latest.json

  mv $TMP/region-hierarchy-latest.json .
  echo "region-hierarchy-latest.json:"
  head region-hierarchy-latest.json

  mv $TMP/provider-country-state-province-latest.json .
  echo "provider-country-state-province-latest.json:"
  head provider-country-state-province-latest.json

  mv $TMP/provider-country-state-province-locations-latest.json .
  echo "provider-country-state-province-locations-latest.json:"
  head provider-country-state-province-locations-latest.json

  mv $TMP/country-state-province-hierarchy-latest.json .
  echo "country-state-province-hierarchy-latest.json:"
  head country-state-province-hierarchy-latest.json

  mv $TMP/synthetic-regions-latest.json .
  echo "synthetic-regions-latest.json:"
  head synthetic-regions-latest.json

  mv $TMP/synthetic-country-state-province-latest.json .
  echo "synthetic-country-state-province-latest.json:"
  head synthetic-country-state-province-latest.json

  mv $TMP/synthetic-locations-latest.json .
  echo "synthetic-locations-latest.json:"
  head synthetic-locations-latest.json

  mv $TMP/synthetic-country-state-province-locations-latest.json .
  echo "synthetic-country-state-province-locations-latest.json:"
  head synthetic-country-state-province-locations-latest.json

  hub bucket push -y
)
