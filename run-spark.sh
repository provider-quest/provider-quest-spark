#! /bin/bash

# ./bin/pyspark --master local[2]

mkdir -p input/miner-power \
  input/miner-info \
  input/asks \
  archive checkpoint
export TZ=UTC
~/projects-jpimac/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit ./miner_power.py 2> ~/tmp/spark-stderr.log | tee ~/tmp/spark.log


