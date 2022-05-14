import os
import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, StringType


def process_asks(spark, suffix=""):

    inputDir = os.environ.get('INPUT_ASKS_DIR') or base_dir + '/' + 'input' + suffix + '/asks'
    outputDir = os.environ.get('OUTPUT_ASKS_DIR') or base_dir + '/output' + suffix + '/asks'
    checkpointDir = os.environ.get('CHECKPOINT_ASKS_DIR') or base_dir + '/checkpoint' + suffix + '/asks'

    schemaAsks = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("miner", "string") \
        .add("seqNo", "integer") \
        .add("askTimestamp", "long") \
        .add("price", "string") \
        .add("verifiedPrice", "string") \
        .add("minPieceSize", "long") \
        .add("maxPieceSize", "long") \
        .add("expiry", "long") \
        .add("error", "string") \
        .add("startTime", "timestamp") \
        .add("endTime", "timestamp")

    asks = spark \
        .readStream \
        .schema(schemaAsks) \
        .json(inputDir) \
        .withWatermark("timestamp", "1 minute")

    asks = asks \
        .withColumn("date", asks.timestamp.astype('date')) \
        .withColumn("priceDouble", asks.price.astype('double')) \
        .withColumn("verifiedPriceDouble", asks.verifiedPrice.astype('double'))

    numberOfAsksRecords = asks.groupBy().count()

    latestAsksSubset = asks \
        .groupBy('miner') \
        .agg(
            last('epoch'),
            last('timestamp'),
            last('price'),
            last('verifiedPrice'),
            last('priceDouble'),
            last('verifiedPriceDouble'),
            last('minPieceSize'),
            last('maxPieceSize'),
            last('askTimestamp'),
            last('expiry'),
            last('seqNo'),
            last('error'))

    """
    queryAsksCounter = numberOfAsksRecords \
        .writeStream \
        .queryName("asks_counter") \
        .outputMode('complete') \
        .format('console') \
        .trigger(processingTime='1 minute') \
        .start()

    queryArchiveAsks = asks \
        .writeStream \
        .queryName("asks_json") \
        .format("json") \
        .option("path", outputDir + "/json") \
        .option("checkpointLocation", checkpointDir + "/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    def output_latest_asks_subset(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/json_latest_subset', mode='overwrite')

    queryLatestAsksSubset = latestAsksSubset \
        .writeStream \
        .queryName("asks_subset_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/json_latest_subset") \
        .foreachBatch(output_latest_asks_subset) \
        .trigger(processingTime='1 minute') \
        .start()
