import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, StringType


def process_miner_power(spark, suffix=""):

    inputDir = 'input' + suffix
    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

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
        .json(inputDir + '/miner-power') \
        .withWatermark("timestamp", "1 minute")

    minerPower = minerPower.withColumn(
        "date", minerPower.timestamp.astype('date'))

    numberOfPowerRecords = minerPower.groupBy().count()

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
        window(minerPower.timestamp, '7 day', '1 day')
    ).avg("rawBytePower", "qualityAdjPower")

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
        .option("path", outputDir + "/miner_power/json") \
        .option("checkpointLocation", checkpointDir + "/miner_power/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgHourly = averagePowerHourly \
        .writeStream \
        .queryName("miner_power_avg_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/miner_power/json_avg_hourly") \
        .option("checkpointLocation", checkpointDir + "/miner_power/json_avg_hourly") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgDaily = averagePowerDaily \
        .writeStream \
        .queryName("miner_power_avg_daily_json") \
        .format("json") \
        .option("path", outputDir + "/miner_power/json_avg_daily") \
        .option("checkpointLocation", checkpointDir + "/miner_power/json_avg_daily") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgMultiday = averagePowerMultiDay \
        .writeStream \
        .queryName("miner_power_avg_multiday_json") \
        .format("json") \
        .option("path", outputDir + "/miner_power/json_avg_multiday") \
        .option("checkpointLocation", checkpointDir + "/miner_power/json_avg_multiday") \
        .partitionBy("window", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    latestPower = minerPower \
        .groupBy(
            'miner'
        ).agg(
            last('epoch'),
            last('timestamp'),
            last('rawBytePower'),
            last('qualityAdjPower')
        )

    def output_latest_power(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/miner_power/json_latest', mode='overwrite')

    queryLatestPower = latestPower \
        .writeStream \
        .queryName("miner_power_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/miner_power/json_latest") \
        .foreachBatch(output_latest_power) \
        .trigger(processingTime='1 minute') \
        .start()


