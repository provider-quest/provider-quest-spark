import os
from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last

def process(minerPower, suffix=""):

    base_dir = os.environ.get('WORK_DIR') or '.'

    output_dir = os.environ.get('OUTPUT_POWER_DIR') or base_dir + '/output' + suffix + '/miner_power'
    checkpoint_dir = os.environ.get('CHECKPOINT_POWER_DIR') or base_dir + '/checkpoint' + suffix + '/miner_power'

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

    averagePowerDailyFlat = averagePowerDaily \
        .drop('window')

    averagePowerMultiDay = minerPower.groupBy(
        minerPower.miner,
        window(minerPower.timestamp, '7 day', '1 day')
    ).avg("rawBytePower", "qualityAdjPower")

    """
    queryPowerCounter = numberOfPowerRecords \
        .writeStream \
        .queryName("miner_power_counter") \
        .outputMode('complete') \
        .format('console') \
        .trigger(processingTime='1 minute') \
        .start()
    """

    """
    queryPowerArchive = minerPower \
        .writeStream \
        .queryName("miner_power_json") \
        .format("json") \
        .option("path", outputDir + "/json") \
        .option("checkpointLocation", checkpointDir + "/json") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    """
    queryPowerAvgHourly = averagePowerHourly \
        .writeStream \
        .queryName("miner_power_avg_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/json_avg_hourly") \
        .option("checkpointLocation", checkpointDir + "/json_avg_hourly") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    queryPowerAvgDaily = averagePowerDaily \
        .writeStream \
        .queryName("miner_power_avg_daily_json") \
        .format("json") \
        .option("path", output_dir + "/json_avg_daily") \
        .option("checkpointLocation", checkpoint_dir + "/json_avg_daily") \
        .partitionBy("date", "miner") \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgDailyCsv = averagePowerDailyFlat \
        .writeStream \
        .queryName("miner_power_avg_daily_csv") \
        .format("csv") \
        .option("path", output_dir + "/csv_avg_daily") \
        .option("checkpointLocation", checkpoint_dir + "/csv_avg_daily") \
        .option("header", True) \
        .trigger(processingTime='1 minute') \
        .start()

    queryPowerAvgMultiday = averagePowerMultiDay \
        .writeStream \
        .queryName("miner_power_avg_multiday_json") \
        .format("json") \
        .option("path", output_dir + "/json_avg_multiday") \
        .option("checkpointLocation", checkpoint_dir + "/json_avg_multiday") \
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
            output_dir + '/json_latest', mode='overwrite')

    queryLatestPower = latestPower \
        .writeStream \
        .queryName("miner_power_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpoint_dir + "/json_latest") \
        .foreachBatch(output_latest_power) \
        .trigger(processingTime='1 minute') \
        .start()


