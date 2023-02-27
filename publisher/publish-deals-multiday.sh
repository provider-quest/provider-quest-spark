#! /bin/bash

set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

TARGET=$WORK_DIR/dist/deals
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
fi

IFS="$(printf '\n\t')"

# Latest multiday deals average
LAST="$(ls -d $OUTPUT_DEALS_DIR/deals/by_provider/aggr_multiday/json/window\=* | sort | tail -1)"
echo $LAST
LAST_ESCAPE=$(echo $LAST | sed 's, ,\\ ,g')
DATE=$(echo $LAST | sed 's,^.*window=%7B\([^ ]*\).*,\1,')
echo $DATE

COUNT=0

# {"provider":"f0756207","count":7,"sum(pieceSizeDouble)":1.88978692096E11,"avg(pieceSizeDouble)":2.6996956013714287E10,"min(pieceSizeDouble)":131072.0,"max(pieceSizeDouble)":3.4359738368E10,"avg(storagePricePerEpochDouble)":1.2574285714285715E10,"min(storagePricePerEpochDouble)":2.0E7,"max(storagePricePerEpochDouble)":1.6E10,"approx_count_distinct(label)":7,"sum(lifetimeValue)":3.9033511694131984,"avg(lifetimeValue)":0.5576215956304569,"min(lifetimeValue)":1.4131982421875E-9,"max(lifetimeValue)":0.743925248,"approx_count_distinct(client)":2}

cat $LAST/part*.json | jq -s " \
{ \
  date: \"$DATE\", \
  intervalDays: 7, \
  providers: map({ \
    key: .provider, \
    value: { \
      count: .count, \
      \"sum(pieceSizeDouble)\": .\"sum(pieceSizeDouble)\", \
      \"avg(pieceSizeDouble)\": .\"avg(pieceSizeDouble)\", \
      \"min(pieceSizeDouble)\": .\"min(pieceSizeDouble)\", \
      \"max(pieceSizeDouble)\": .\"max(pieceSizeDouble)\", \
      \"avg(storagePricePerEpochDouble)\": .\"avg(storagePricePerEpochDouble)\", \
      \"min(storagePricePerEpochDouble)\": .\"min(storagePricePerEpochDouble)\", \
      \"max(storagePricePerEpochDouble)\": .\"max(storagePricePerEpochDouble)\", \
      \"approx_count_distinct(label)\": .\"approx_count_distinct(label)\", \
      \"sum(lifetimeValue)\": .\"sum(lifetimeValue)\", \
      \"avg(lifetimeValue)\": .\"avg(lifetimeValue)\", \
      \"min(lifetimeValue)\": .\"min(lifetimeValue)\", \
      \"max(lifetimeValue)\": .\"max(lifetimeValue)\", \
      \"approx_count_distinct(client)\": .\"approx_count_distinct(client)\" \
    } \
  }) | from_entries \
} \
" > $TMP/multiday-average-latest.json

(
  set -e

  cd $TARGET

  mv $TMP/multiday-average-latest.json .
  echo "multiday-average-latest.json:"
  head multiday-average-latest.json
)


