from pyspark.sql.functions import window
from pyspark.sql.functions import last

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

    # Average Power

    averagePowerDaily = minerPowerWithRegions.groupBy(
        minerPowerWithRegions.region,
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
