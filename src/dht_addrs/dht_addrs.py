import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, StringType


def process_dht_addrs(spark, suffix=""):

    inputDir = 'input' + suffix
    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    schemaDhtAddrs = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("collectedFrom", "string") \
        .add("miner", "string") \
        .add("peerId", "string") \
        .add("multiaddrs", ArrayType(StringType()))

    dhtAddrs = spark \
        .readStream \
        .schema(schemaDhtAddrs) \
        .json(inputDir + '/dht-addrs') \
        .withWatermark("timestamp", "1 minute")

    dhtAddrs = dhtAddrs.withColumn(
        "date", dhtAddrs.timestamp.astype('date'))

    queryArchiveDhtAddrs = dhtAddrs \
        .writeStream \
        .queryName("dht_addrs_json") \
        .format("json") \
        .option("path", outputDir + "/dht-addrs/json") \
        .option("checkpointLocation", checkpointDir + "/dht-addrs/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    latestDhtAddrsSubset = dhtAddrs \
        .groupBy('miner') \
        .agg(
            last('epoch'),
            last('timestamp'),
            last('collectedFrom'),
            last('peerId'),
            last('multiaddrs'))

    def output_latest_dht_addrs_subset(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/dht-addrs/json_latest_subset', mode='overwrite')

    queryLatestDhtAddrsSubset = latestDhtAddrsSubset \
        .writeStream \
        .queryName("dht_addrs_subset_latest_json") \
        .outputMode('complete') \
        .foreachBatch(output_latest_dht_addrs_subset) \
        .trigger(processingTime='1 minute') \
        .start()
