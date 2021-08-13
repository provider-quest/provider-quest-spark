from pyspark.sql.functions import window
from pyspark.sql.functions import count
from pyspark.sql.functions import sum
from pyspark.sql.functions import avg
from pyspark.sql.functions import when


def process(deals, minerRegions, suffix=""):

    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    dealsWithRegions = deals \
        .drop("hour", "clientProvider") \
        .join(
            minerRegions,
            deals.provider == minerRegions.miner,
            how='leftOuter'
        ).fillna('none', 'region')

    dealsWithRegions = dealsWithRegions \
        .withColumn("splitPieceSizeDouble",
                    dealsWithRegions.pieceSizeDouble /
                    dealsWithRegions.numRegions) \
        .withColumn("splitLifetimeValue",
                    dealsWithRegions.lifetimeValue /
                    dealsWithRegions.numRegions)

    # Archive

    queryArchiveDealsByRegion = dealsWithRegions \
        .writeStream \
        .queryName("deals_by_miner_region_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_miner_region/archive/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_miner_region/archive/json") \
        .partitionBy("region", "date") \
        .trigger(processingTime='1 minute') \
        .start()
