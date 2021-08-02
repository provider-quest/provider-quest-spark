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
~/projects-jpimac/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit ./pyspark_main.py 2> ~/tmp/spark-stderr.log | tee ~/tmp/spark.log


