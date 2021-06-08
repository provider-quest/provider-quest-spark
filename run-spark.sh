#! /bin/bash

# ./bin/pyspark --master local[2]

mkdir -p input archive checkpoint
export TZ=UTC
~/projects-jpimac/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit ./miner_power.py 2> /dev/null


