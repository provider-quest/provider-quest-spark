import os
import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, MapType, StringType


def process_dht_addrs(spark, suffix=""):

    inputDir = os.environ.get('INPUT_DHT_ADDRS_DIR') or \
        'input' + suffix + '/dht-addrs'
    outputDir = os.environ.get('OUTPUT_DHT_ADDRS_DIR') or \
        '../work/output' + suffix + '/dht-addrs'
    checkpointDir = os.environ.get('CHECKPOINT_DHT_ADDRS_DIR') or \
        '../work/checkpoint' + suffix + '/dht-addrs'

    schemaDhtAddrs = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("collectedFrom", "string") \
        .add("miner", "string") \
        .add("peerId", "string") \
        .add("multiaddrs", ArrayType(StringType())) \
        .add("dnsLookups", MapType(
            StringType(),
            ArrayType(StringType())
        ))

    dhtAddrs = spark \
        .readStream \
        .schema(schemaDhtAddrs) \
        .json(inputDir) \
        .withWatermark("timestamp", "1 minute")

    dhtAddrs = dhtAddrs.withColumn(
        "date", dhtAddrs.timestamp.astype('date'))

    """
    queryArchiveDhtAddrs = dhtAddrs \
        .writeStream \
        .queryName("dht_addrs_json") \
        .format("json") \
        .option("path", outputDir + "/json") \
        .option("checkpointLocation", checkpointDir + "/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    latestDhtAddrsSubset = dhtAddrs \
        .groupBy('miner') \
        .agg(
            last('epoch'),
            last('timestamp'),
            last('collectedFrom'),
            last('peerId'),
            last('multiaddrs'),
            last('dnsLookups'))

    def output_latest_dht_addrs_subset(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/json_latest_subset', mode='overwrite')

    queryLatestDhtAddrsSubset = latestDhtAddrsSubset \
        .writeStream \
        .queryName("dht_addrs_subset_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/json_latest_subset") \
        .foreachBatch(output_latest_dht_addrs_subset) \
        .trigger(processingTime='1 minute') \
        .start()

    countsDaily = dhtAddrs \
        .groupBy(
            dhtAddrs.miner,
            dhtAddrs.date,
            window(dhtAddrs.timestamp, '1 day')
        ).count()

    queryCountsDaily = countsDaily \
        .writeStream \
        .queryName("dht_addrs_counts_daily_json") \
        .format("json") \
        .option("path", outputDir + "/json_counts_daily") \
        .option("checkpointLocation", checkpointDir + "/json_counts_daily") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    countsMultiday = dhtAddrs \
        .groupBy(
            dhtAddrs.miner,
            window(dhtAddrs.timestamp, '7 days', '1 day')
        ).count()

    queryCountsMultiday = countsMultiday \
        .writeStream \
        .queryName("dht_addrs_counts_multiday_json") \
        .format("json") \
        .option("path", outputDir + "/json_counts_multiday") \
        .option("checkpointLocation", checkpointDir + "/dht_addrs/json_counts_multiday") \
        .partitionBy("window") \
        .trigger(processingTime='1 minute') \
        .start()

