#! /bin/bash

#set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

TARGET=$WORK_DIR/dist/deals-daily
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
        cd $TARGET
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

PART=$(ls $OUTPUT_DEALS_DIR/deals/by_provider/by_verified/aggr_daily/csv/part*.csv | head -1)
head -1 $PART > $TMP/deals-daily.csv
for f in $OUTPUT_DEALS_DIR/deals/by_provider/by_verified/aggr_daily/csv/part*.csv; do
  echo $f
  grep ^2 $f >> $TMP/deals-daily.csv
done

cd $TARGET
mv $TMP/deals-daily.csv .
echo 'deals-daily.csv:'
head deals-daily.csv

