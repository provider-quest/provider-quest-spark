#! /bin/bash

# ./bin/pyspark --master local[2]

mkdir -p input-staging/miner-power \
  input-staging/miner-info \
  input-staging/asks \
  input-staging/deals \
  input-staging/dht-addrs \
  input-staging/multiaddrs-ips \
  input-staging/ips-geolite2 \
  archive-staging \
  checkpoint-staging

rsync -vaP estuary-archive/client-names input-staging
rsync -vaP estuary-archive/miner-regions input-staging
rsync -vaP estuary-archive/miner-locations input-staging

export TZ=UTC

~/projects-jpimac/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit ./pyspark_main_staging.py 2> ~/tmp/spark-staging-stderr.log | tee ~/tmp/spark-staging.log


