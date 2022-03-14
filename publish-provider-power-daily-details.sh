#! /bin/bash

if [ ! -f PUBLISH ]; then
	echo Skipping publishing, PUBLISH file is missing
	exit
fi

IFS="$(printf '\n\t')"
DATE=$(node -e 'console.log((new Date()).toISOString())')

mkdir -p dist/miner-power-daily

PART=$(ls ../work/output/miner_power/csv_avg_daily/part*.csv | head -1)
head -1 $PART | sed 's/miner/provider/' > tmp/provider-power-daily.csv
for f in ../work/output/miner_power/csv_avg_daily/part*.csv; do
  echo $f
  grep ^f $f >> tmp/provider-power-daily.csv
done

cd dist/miner-power-daily
#hub bucket pull -y
mv ../../tmp/provider-power-daily.csv provider-power-daily.csv
head provider-power-daily.csv
hub bucket push -y

