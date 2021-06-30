import os
import sys
import time

from pyspark.sql import SparkSession

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from deals import deals

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerPowerStaging")\
        .getOrCreate()

    deals.process_deals(spark, '-staging')

    while True:
        for stream in spark.streams.active:
            if stream.status['message'] != "Waiting for data to arrive":
                print(stream.name, stream.status['message'])
        time.sleep(1)
