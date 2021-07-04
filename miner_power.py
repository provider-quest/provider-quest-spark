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

from deals import deals

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerPower")\
        .getOrCreate()

    schemaPower = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("tipSet", "string") \
        .add("miner", "string") \
        .add("rawBytePower", "double") \
        .add("qualityAdjPower", "double")

    minerPower = spark \
        .readStream \
        .schema(schemaPower) \
        .json('input/miner-power') \
        .withWatermark("timestamp", "1 minute")

    minerPower = minerPower.withColumn(
        "date", minerPower.timestamp.astype('date'))

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
        .add("consensusFaultElapsed", "long")

    minerInfo = spark \
        .readStream \
        .schema(schemaInfo) \
        .json('input/miner-info') \
        .withWatermark("timestamp", "1 minute")

    minerInfo = minerInfo.withColumn(
        "date", minerInfo.timestamp.astype('date'))

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
        .json('input/asks') \
        .withWatermark("timestamp", "1 minute")

    asks = asks \
        .withColumn("date", asks.timestamp.astype('date')) \
        .withColumn("priceDouble", asks.price.astype('double')) \
        .withColumn("verifiedPriceDouble", asks.verifiedPrice.astype('double'))

    numberOfPowerRecords = minerPower.groupBy().count()
    numberOfInfoRecords = minerInfo.groupBy().count()
    numberOfAsksRecords = asks.groupBy().count()

    averagePowerHourly = minerPower.groupBy(
        minerPower.miner,
        minerPower.date,
        window(minerPower.timestamp, '1 hour')
    ).avg("rawBytePower", "qualityAdjPower")

    averagePowerDaily = minerPower.groupBy(
        minerPower.miner,
        minerPower.date,
        window(minerPower.timestamp, '1 day')
    ).avg("rawBytePower", "qualityAdjPower")

    averagePowerMultiDay = minerPower.groupBy(
        minerPower.miner,
        window(minerPower.timestamp, '2 day', '2 day')
    ).avg("rawBytePower", "qualityAdjPower")

    latestMinerInfoSubset = minerInfo \
        .groupBy('miner') \
        .agg(last('sectorSize'), last('peerId'), last('multiaddrsDecoded'))

    latestAsksSubset = asks \
        .groupBy('miner') \
        .agg(
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

    queryPowerCounter = numberOfPowerRecords \
        .writeStream \
        .queryName("miner_power_counter") \
        .outputMode('complete') \
        .format('console') \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerArchive = minerPower \
        .writeStream \
        .queryName("miner_power_json") \
        .format("json") \
        .option("path", "output/miner_power/json") \
        .option("checkpointLocation", "checkpoint/miner_power/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgHourly = averagePowerHourly \
        .writeStream \
        .queryName("miner_power_avg_hourly_json") \
        .format("json") \
        .option("path", "output/miner_power/json_avg_hourly") \
        .option("checkpointLocation", "checkpoint/miner_power/json_avg_hourly") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgDaily = averagePowerDaily \
        .writeStream \
        .queryName("miner_power_avg_daily_json") \
        .format("json") \
        .option("path", "output/miner_power/json_avg_daily") \
        .option("checkpointLocation", "checkpoint/miner_power/json_avg_daily") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgMultiday = averagePowerMultiDay \
        .writeStream \
        .queryName("miner_power_avg_multiday_json") \
        .format("json") \
        .option("path", "output/miner_power/json_avg_multiday") \
        .option("checkpointLocation", "checkpoint/miner_power/json_avg_multiday") \
        .partitionBy("window", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

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
        .option("path", "output/miner_info/json") \
        .option("checkpointLocation", "checkpoint/miner_info/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    def output_latest_miner_info_subset(df, epoch_id):
        df.coalesce(1).write.json(
            'output/miner_info/json_latest_subset', mode='overwrite')

    queryMinerInfoSubsetLatest = latestMinerInfoSubset \
        .writeStream \
        .queryName("miner_info_subset_latest_json") \
        .outputMode('complete') \
        .foreachBatch(output_latest_miner_info_subset) \
        .trigger(processingTime='1 minute') \
        .start()

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
        .option("path", "output/asks/json") \
        .option("checkpointLocation", "checkpoint/asks/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    def output_latest_asks_subset(df, epoch_id):
        df.coalesce(1).write.json(
            'output/asks/json_latest_subset', mode='overwrite')

    queryLatestAsksSubset = latestAsksSubset \
        .writeStream \
        .queryName("asks_subset_latest_json") \
        .outputMode('complete') \
        .foreachBatch(output_latest_asks_subset) \
        .trigger(processingTime='1 minute') \
        .start()

    deals.process_deals(spark)

    while True:
        for stream in spark.streams.active:
            message = stream.status['message']
            if message != "Waiting for data to arrive" and \
                    message.find("Getting offsets") == -1:
                print(stream.name, message)
        time.sleep(10)
