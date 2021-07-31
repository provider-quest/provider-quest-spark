from pyspark.sql.functions import window
from pyspark.sql.functions import count
from pyspark.sql.functions import sum

def process(minerPower, minerRegions, suffix=""):

    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    minerPowerWithRegions = minerPower.join( minerRegions, [ 'miner' ])

    minerPowerWithRegions = minerPowerWithRegions \
        .withColumn("splitRawBytePower", 
            minerPowerWithRegions.rawBytePower /
            minerPowerWithRegions.numRegions) \
        .withColumn("splitQualityAdjPower", 
            minerPowerWithRegions.qualityAdjPower /
            minerPowerWithRegions.numRegions)

    # Archive

    queryArchiveDealsByRegion = minerPowerWithRegions \
        .writeStream \
        .queryName("miner_power_by_miner_region_json") \
        .format("json") \
        .option("path", outputDir + "/miner_power/by_miner_region/archive/json") \
        .option("checkpointLocation", checkpointDir + "/miner_power/by_miner_region/archive/json") \
        .partitionBy("region", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    # Summed Average Power

    averagePowerDaily = minerPowerWithRegions.groupBy(
        minerPowerWithRegions.region,
        minerPowerWithRegions.miner,
        minerPowerWithRegions.date,
        window('timestamp', '1 day')
    ).avg(
        "splitRawBytePower",
        "splitQualityAdjPower"
    )

    queryPowerAvgDaily = averagePowerDaily \
        .writeStream \
        .queryName("miner_power_by_miner_region_avg_daily_json") \
        .format("json") \
        .option("path", outputDir + "/miner_power/by_miner_region/avg_daily/json") \
        .option("checkpointLocation", checkpointDir + "/miner_power/by_miner_region/avg_daily/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    def output_summed(df, epoch_id):
        summedDf = df.groupBy(
            'date',
            'region'
        ).agg(
            count('miner'),
            sum('avg(splitRawBytePower)'),
            sum('avg(splitQualityAdjPower)')
        )

        # summedDf.coalesce(1).write.partitionBy('date').json(
        summedDf.orderBy('date', 'region').coalesce(1).write.json(
                outputDir + '/miner_power/by_miner_region/sum_avg_daily/json',
                mode='overwrite')

    queryPowerSumAvgDaily = averagePowerDaily \
        .writeStream \
        .queryName("miner_power_by_miner_region_sum_avg_daily_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/miner_power/by_miner_region/sum_avg_daily/json") \
        .foreachBatch(output_summed) \
        .trigger(processingTime='1 minute') \
        .start()

