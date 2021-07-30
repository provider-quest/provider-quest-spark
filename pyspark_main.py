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
from deals import deals_source
from deals import deals_base
from deals import deals_by_provider
from deals import deals_by_client
from deals import deals_by_pairs
from deals import deals_sample
from deals import deals_client_names
from client_names import client_names
from asks import asks
from dht_addrs import dht_addrs
from multiaddrs_ips import multiaddrs_ips
from ips_geolite2 import ips_geolite2

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerReport")\
        .getOrCreate()

    suffix = ''

    miner_power.process_miner_power(spark)

    miner_info.process_miner_info(spark)

    names = client_names.process_client_names(spark)

    deals = deals_source.get(spark, suffix)
    deals_base.process(deals, suffix)
    deals_by_provider.process(deals, suffix)
    deals_by_client.process(deals, suffix)
    deals_by_pairs.process(deals, suffix)
    deals_sample.process(deals, suffix)
    deals_client_names.process(deals, names, suffix)

    asks.process_asks(spark)

    dht_addrs.process_dht_addrs(spark)

    multiaddrs_ips.process_multiaddrs_ips(spark)

    ips_geolite2.process_ips_geolite2(spark)

    while True:
        for stream in spark.streams.active:
            message = stream.status['message']
            if message != "Waiting for data to arrive" and \
                    message != "Waiting for next trigger" and \
                    message.find("Getting offsets") == -1:
                print(stream.name, message)
        time.sleep(10)
