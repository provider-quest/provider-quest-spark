import os
import sys
import time

from pyspark.sql import SparkSession

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from miner_power import miner_power
from deals import deals
from client_names import client_names

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerPowerStaging")\
        .getOrCreate()

    suffix = '-staging'

    miner_power.process_miner_power(spark, suffix)

    #names = client_names.process_client_names(spark, suffix)

    #deals.process_deals(spark, names, suffix)

    while True:
        for stream in spark.streams.active:
            if stream.status['message'] != "Waiting for data to arrive":
                print(stream.name, stream.status['message'])
        time.sleep(1)
