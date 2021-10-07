import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, StringType


def process_asks(spark, suffix=""):

    inputDir = 'input' + suffix
    outputDir = '../work/output' + suffix
    checkpointDir = '../work/checkpoint' + suffix

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
        .json(inputDir + '/asks') \
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
        .option("path", outputDir + "/asks/json") \
        .option("checkpointLocation", checkpointDir + "/asks/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    def output_latest_asks_subset(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/asks/json_latest_subset', mode='overwrite')

    queryLatestAsksSubset = latestAsksSubset \
        .writeStream \
        .queryName("asks_subset_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/asks/json_latest_subset") \
        .foreachBatch(output_latest_asks_subset) \
        .trigger(processingTime='1 minute') \
        .start()
