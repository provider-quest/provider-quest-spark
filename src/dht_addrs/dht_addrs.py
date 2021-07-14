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

