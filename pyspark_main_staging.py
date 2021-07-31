import os
import sys
import time

from pyspark.sql import SparkSession

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from miner_power import miner_power_source
from miner_power import miner_power_base
from miner_power import miner_power_regions
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
from miner_regions import miner_regions

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerReportStaging")\
        .getOrCreate()

    suffix = '-staging'

    minerPower = miner_power_source.get(spark, suffix)
    miner_power_base.process(minerPower, suffix)

    #miner_info.process_miner_info(spark, suffix)

    names = client_names.get(spark, suffix)

    minerRegions = miner_regions.get_latest(spark, suffix)
    miner_power_regions.process(minerPower, minerRegions, suffix)

    deals = deals_source.get(spark, suffix)
    deals_base.process(deals, suffix)
    deals_by_provider.process(deals, suffix)
    deals_by_client.process(deals, suffix)
    deals_by_pairs.process(deals, suffix)
    deals_sample.process(deals, suffix)
    deals_client_names.process(deals, names, suffix)

    #deals.process_deals(spark, names, suffix)

    #asks.process_asks(spark, suffix)

    #dht_addrs.process_dht_addrs(spark, suffix)

    #multiaddrsIps = multiaddrs_ips.process_multiaddrs_ips(spark, suffix)

    #ipsGeoLite2 = ips_geolite2.process_ips_geolite2(spark, suffix)

    while True:
        for stream in spark.streams.active:
            message = stream.status['message']
            if message != "Waiting for data to arrive" and \
                    message != "Waiting for next trigger" and \
                    message.find("Getting offsets") == -1:
                print(stream.name, message)
        time.sleep(1)
