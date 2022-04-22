import os
from pyspark.sql.functions import window
from pyspark.sql.functions import count
from pyspark.sql.functions import sum
from pyspark.sql.functions import avg
from pyspark.sql.functions import when


def process(minerPower, providerCountryStateProvinces, suffix=""):

    outputDir = os.environ.get('OUTPUT_POWER_CSP_REGIONS_DIR') or \
        '../work/output' + suffix + '/miner_power/by_provider_country_state_province'
    checkpointDir = os.environ.get('CHECKPOINT_POWER_CSP_REGIONS_DIR') or \
        '../work/checkpoint' + suffix + '/miner_power/by_provider_country_state_province'

    minerPower = minerPower.where(
        "rawBytePower > 0 OR qualityAdjPower > 0"
    )

    minerPowerWithRegions = minerPower.join(
        providerCountryStateProvinces,
        on='miner',
        how='leftOuter'
    ).fillna('none', 'region')

    minerPowerWithRegions = minerPowerWithRegions \
        .withColumn("splitCount",
                    when(minerPowerWithRegions.region == 'none', 1.0) \
                    .otherwise(1.0 / minerPowerWithRegions.numRegions)) \
        .withColumn("splitRawBytePower",
                    minerPowerWithRegions.rawBytePower /
                    minerPowerWithRegions.numRegions) \
        .withColumn("splitQualityAdjPower",
                    minerPowerWithRegions.qualityAdjPower /
                    minerPowerWithRegions.numRegions)

    # Archive

    """
    queryArchiveDealsByRegion = minerPowerWithRegions \
        .writeStream \
        .queryName("miner_power_by_provider_country_state_province_json") \
        .format("json") \
        .option("path", outputDir + "/archive/json") \
        .option("checkpointLocation", checkpointDir + "/archive/json") \
        .partitionBy("region", "date") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    # Summed Average Power

    averagePowerDaily = minerPowerWithRegions.groupBy(
        minerPowerWithRegions.miner,
        minerPowerWithRegions.date,
        minerPowerWithRegions.region,
        minerPowerWithRegions.splitCount,
        window('timestamp', '1 day')
    ).agg(
        avg("rawBytePower"),
        avg("qualityAdjPower"),
        avg("splitRawBytePower"),
        avg("splitQualityAdjPower")
    )

    averagePowerDaily = averagePowerDaily \
        .withColumn('rawBytePower', when(
            minerPowerWithRegions.region == 'none',
            averagePowerDaily['avg(rawBytePower)']
        ).otherwise(averagePowerDaily['avg(splitRawBytePower)'])
        ) \
        .withColumn('qualityAdjPower', when(
            minerPowerWithRegions.region == 'none',
            averagePowerDaily['avg(qualityAdjPower)']
        ).otherwise(averagePowerDaily['avg(splitQualityAdjPower)'])
        )

    queryPowerAvgDaily = averagePowerDaily \
        .writeStream \
        .queryName("miner_power_by_provider_country_state_province_avg_daily_json") \
        .format("json") \
        .option("path", outputDir + "/avg_daily/json") \
        .option("checkpointLocation", checkpointDir + "/avg_daily/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    def output_summed(df, epoch_id):
        summedDf = df.groupBy(
            'date',
            'region'
        ).agg(
            count('miner'),
            sum('splitCount'),
            sum('rawBytePower'),
            sum('qualityAdjPower')
        )

        # summedDf.coalesce(1).write.partitionBy('date').json(
        summedDf.orderBy('date', 'region').coalesce(1).write.json(
            outputDir + '/sum_avg_daily/json',
            mode='overwrite')

    queryPowerSumAvgDaily = averagePowerDaily \
        .writeStream \
        .queryName("miner_power_by_provider_country_state_province_sum_avg_daily_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/sum_avg_daily/json") \
        .foreachBatch(output_summed) \
        .trigger(processingTime='1 minute') \
        .start()
