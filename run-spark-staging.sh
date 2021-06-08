#! /bin/bash

# ./bin/pyspark --master local[2]

mkdir -p input-staging archive-staging checkpoint-staging
export TZ=UTC
~/projects-jpimac/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit ./miner_power_staging.py 2>&1 | tee ~/tmp/spark-staging.log


