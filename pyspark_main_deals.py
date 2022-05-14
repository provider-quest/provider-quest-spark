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

from deals import deals_source
from deals import deals_base
from deals import deals_by_provider
from deals import deals_by_client
from deals import deals_by_pairs
from deals import deals_sample
from deals import deals_client_names
from deals import deals_regions
from deals import deals_synthetic_regions
from deals import deals_country_state_province
from deals import deals_synthetic_csp_regions
from client_names import client_names
from miner_regions import miner_regions
from synthetic_regions import synthetic_regions
from synthetic_regions import synthetic_csp_regions
from provider_country_state_province import provider_country_state_province
from asks import asks

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerReport")\
        .getOrCreate()

    suffix = ''

    names = client_names.get(spark, suffix)

    minerRegions = miner_regions.get_latest(spark, suffix)
    syntheticRegions = synthetic_regions.get_latest(spark, suffix)
    providerCountryStateProvinces = provider_country_state_province.get_latest(spark, suffix)
    syntheticCSPRegions = synthetic_csp_regions.get_latest(spark, suffix)

    deals = deals_source.get(spark, suffix)
    deals_base.process(deals, suffix)
    deals_by_provider.process(deals, suffix)
    deals_by_client.process(deals, suffix)
    deals_by_pairs.process(deals, suffix)
    deals_sample.process(deals, suffix)
    deals_client_names.process(deals, names, suffix)
    deals_regions.process(deals, minerRegions, suffix)
    deals_synthetic_regions.process(deals, syntheticRegions, suffix)
    deals_country_state_province.process(deals, providerCountryStateProvinces, suffix)
    deals_synthetic_csp_regions.process(deals, syntheticCSPRegions, suffix)

    asks.process_asks(spark, suffix)

    while True:
        for stream in spark.streams.active:
            message = stream.status['message']
            if message != "Waiting for data to arrive" and \
                    message != "Waiting for next trigger" and \
                    message.find("Getting offsets") == -1:
                print(stream.name, message)
        time.sleep(10)
