import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct
from pyspark.sql.functions import hour
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StructType, ArrayType, StringType


def process(multiaddrsIps, ipsGeoLite2, suffix=""):

    inputDir = 'input' + suffix
    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    multiaddrsGeoLite2 = multiaddrsIps.join(
      ipsGeoLite2,
      multiaddrsIps.ip == ipsGeoLite2.ip
    )

    #queryArchiveMultiaddrsGeoLite2 = multiaddrsGeoLite2 \
    #  .writeStream \
    #  .queryName("multiaddrs_geolite2_json") \
    #  .format("json") \
    #  .option("path", outputDir + "/multiaddrs_geolite2/json") \
    #  .option("checkpointLocation", checkpointDir + "/multiaddrs_geolite2/json") \
    #  .partitionBy("miner", "date") \
    #  .trigger(processingTime='1 minute') \
    #  .start()

    #latestMultiaddrsGeoLite2 = multiaddrsGeoLite2 \
    #  .groupBy(
    #    'miner',
    #    'maddr',
    #    'peerId',
    #    multiaddrsIps.ip,
    #    # multiaddrsIps.timestamp,
    #    #multiaddrsIps.epoch
    #  ).agg(
    #    last('chain'),
    #    last('dht'),
    #    last('continent'),
    #    last('country'),
    #    last('subdiv1'),
    #    last('city'),
    #    last('long'),
    #    last('lat')
    #  )

    #def output_latest_multiaddrs_geolite2(df, epoch_id):
    #    df.coalesce(1).write.json(
    #        outputDir + '/multiaddrs_geolite2/json_latest', mode='overwrite')

    #queryLatestMultiaddrsGeoLite2 = latestMultiaddrsGeoLite2 \
    #    .writeStream \
    #    .queryName("multiaddrs_geolite2_latest_json") \
    #    .outputMode('append') \
    #    .option("checkpointLocation", checkpointDir + "/multiaddrs_geolite2/json_latest") \
    #    .foreachBatch(output_latest_multiaddrs_geolite2) \
    #    .trigger(processingTime='1 minute') \
    #    .start()

