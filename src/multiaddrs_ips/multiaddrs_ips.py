import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, MapType, StringType


def process_multiaddrs_ips(spark, suffix=""):

    inputDir = 'input' + suffix
    outputDir = '../work/output' + suffix
    checkpointDir = '../work/checkpoint' + suffix

    schemaMultiaddrsIps = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("miner", "string") \
        .add("maddr", "string") \
        .add("peerId", "string") \
        .add("ip", "string") \
        .add("chain", "boolean") \
        .add("dht", "boolean")

    multiaddrsIps = spark \
        .readStream \
        .schema(schemaMultiaddrsIps) \
        .json(inputDir + '/multiaddrs-ips') \
        .withWatermark("timestamp", "1 minute")

    multiaddrsIps = multiaddrsIps.withColumn(
        "date", multiaddrsIps.timestamp.astype('date'))

    queryArchiveMultiaddrsIps = multiaddrsIps \
        .writeStream \
        .queryName("multiaddrs_ips_json") \
        .format("json") \
        .option("path", outputDir + "/multiaddrs_ips/json") \
        .option("checkpointLocation", checkpointDir + "/multiaddrs_ips/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    latestMultiaddrsIpsSubset = multiaddrsIps \
        .groupBy(
          'miner',
          'maddr',
          'peerId',
          'ip'
        ).agg(
          last('epoch'),
          last('timestamp'),
          last('chain'),
          last('dht')
        )

    def output_latest_multiaddrs_ips_subset(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/multiaddrs_ips/json_latest_subset', mode='overwrite')

    queryLatestMultiaddrsIpsSubset = latestMultiaddrsIpsSubset \
        .writeStream \
        .queryName("multiaddrs_ips_subset_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/multiaddrs_ips/json_latest_subset") \
        .foreachBatch(output_latest_multiaddrs_ips_subset) \
        .trigger(processingTime='1 minute') \
        .start()

    return multiaddrsIps


