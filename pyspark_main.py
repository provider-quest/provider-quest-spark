import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, StringType

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from miner_power import miner_power
from miner_info import miner_info
from deals import deals
from client_names import client_names
from asks import asks
from dht_addrs import dht_addrs

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerPower")\
        .getOrCreate()

    miner_power.process_miner_power(spark)

    miner_info.process_miner_info(spark)

    names = client_names.process_client_names(spark)

    deals.process_deals(spark, names)

    asks.process_asks(spark)

    dht_addrs.process_dht_addrs(spark)

    while True:
        for stream in spark.streams.active:
            message = stream.status['message']
            if stream.status['message'] != "Waiting for data to arrive" and \
                    stream.status['message'] != "Waiting for next trigger" and \
                    message.find("Getting offsets") == -1:
                print(stream.name, message)
        time.sleep(10)