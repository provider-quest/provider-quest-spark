import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, MapType, StringType


def process_miner_info(spark, suffix=""):

    inputDir = 'input' + suffix
    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    schemaInfo = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("tipSet", "string") \
        .add("miner", "string") \
        .add("owner", "string") \
        .add("worker", "string") \
        .add("newWorker", "string") \
        .add("controlAddresses", ArrayType(StringType())) \
        .add("peerId", "string") \
        .add("multiaddrs", ArrayType(StringType())) \
        .add("multiaddrsDecoded", ArrayType(StringType())) \
        .add("windowPoStProofType", "short") \
        .add("sectorSize", "long") \
        .add("windowPoStPartitionSectors", "long") \
        .add("consensusFaultElapsed", "long") \
        .add("dnsLookups", MapType(
            StringType(),
            ArrayType(StringType())
        ))

    minerInfo = spark \
        .readStream \
        .schema(schemaInfo) \
        .json(inputDir + '/miner-info') \
        .withWatermark("timestamp", "1 minute")

    minerInfo = minerInfo.withColumn(
        "date", minerInfo.timestamp.astype('date'))

    numberOfInfoRecords = minerInfo.groupBy().count()

    latestMinerInfoSubset = minerInfo \
        .groupBy('miner') \
        .agg(
            last('epoch'), \
            last('timestamp'), \
            last('sectorSize'), \
            last('peerId'), \
            last('multiaddrsDecoded'), \
            last('dnsLookups') \
        )

    queryInfoCounter = numberOfInfoRecords \
        .writeStream \
        .queryName("miner_info_counter") \
        .outputMode('complete') \
        .format('console') \
        .trigger(processingTime='1 minute') \
        .start()

    queryMinerInfoArchive = minerInfo \
        .writeStream \
        .queryName("miner_info_json") \
        .format("json") \
        .option("path", outputDir + "/miner_info/json") \
        .option("checkpointLocation", checkpointDir + "/miner_info/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    def output_latest_miner_info_subset(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/miner_info/json_latest_subset', mode='overwrite')

    queryMinerInfoSubsetLatest = latestMinerInfoSubset \
        .writeStream \
        .queryName("miner_info_subset_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/miner_info/json_latest_subset") \
        .foreachBatch(output_latest_miner_info_subset) \
        .trigger(processingTime='1 minute') \
        .start()

