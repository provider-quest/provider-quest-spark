#! /bin/bash

# ./bin/pyspark --master local[2]

mkdir -p $WORK_DIR/tmp

export TZ=UTC
export TIMESTAMP=$(date +'%s')

while true; do
  date
  timeout 6h \
	  /opt/spark/spark-3.2.1-bin-hadoop3.2/bin/spark-submit \
	  --driver-memory 10G \
	  --executor-memory 4G \
	  ./pyspark_main_deals.py \
	  2> $WORK_DIR/tmp/spark-stderr-$TIMESTAMP.log \
	  | tee -a $WORK_DIR/tmp/spark-$TIMESTAMP.log
  killall java 2> /dev/null
  echo
  echo Sleeping for 60 seconds...
  sleep 60
  killall -9 java 2> /dev/null
  echo
done

