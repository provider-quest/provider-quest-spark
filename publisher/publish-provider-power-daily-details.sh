#! /bin/bash

#set -e
set +x

TMP=$WORK_DIR/tmp
mkdir -p $TMP

./setup-textile.sh

TARGET=$WORK_DIR/dist/miner-power-daily
if [ ! -d $TARGET ]; then
        mkdir -p $TARGET
        cd $TARGET
        hub bucket init \
                --thread $TEXTILE_BUCKET_THREAD \
                --key $BUCKET_MINER_POWER_DAILY_KEY
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

PART=$(ls $OUTPUT_POWER_DIR/csv_avg_daily/part*.csv | head -1)
head -1 $PART | sed 's/miner/provider/' > $TMP/provider-power-daily.csv
for f in $OUTPUT_POWER_DIR/csv_avg_daily/part*.csv; do
  echo $f
  grep ^f $f >> $TMP/provider-power-daily.csv
done

cd $TARGET
hub bucket pull -y
mv $TMP/provider-power-daily.csv .
echo 'provider-power-daily.csv:'
head provider-power-daily.csv
hub bucket push -y

