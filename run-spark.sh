#! /bin/bash

# ./bin/pyspark --master local[2]

mkdir -p \
  input/miner-power \
  input/miner-info \
  input/asks \
  input/deals \
  input/dht-addrs \
  input/multiaddrs-ips \
  input/ips-geolite2 \
  input/ips-baidu \
  archive \
  checkpoint

export TZ=UTC
export TIMESTAMP=$(date +'%s')

while true; do
  date
  timeout 6h ../spark-3.1.2-bin-hadoop3.2/bin/spark-submit ./pyspark_main.py 2> ~/tmp/spark-stderr-$TIMESTAMP.log | tee -a ~/tmp/spark-$TIMESTAMP.log
  killall java 2> /dev/null
  echo
  echo Sleeping for 60 seconds...
  sleep 60
  killall -9 java 2> /dev/null
  echo
done


