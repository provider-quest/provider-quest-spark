import os
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType

def get_latest(spark, suffix=""):

    inputDir = 'input' + suffix

    candidates = os.listdir(inputDir + '/synthetic-regions')
    epoch = sorted([int(n) for n in candidates])[-1]

    schemaSyntheticRegions = StructType() \
        .add("provider", "string") \
        .add("region", "string") \
        .add("numRegions", "short")

    syntheticRegions = spark \
        .read \
        .schema(schemaSyntheticRegions) \
        .json(f"{inputDir}/synthetic-regions/{epoch}/synthetic-provider-country-state-province-{epoch}.json")

    syntheticRegions = syntheticRegions.withColumn("syntheticRegionEpoch", lit(epoch))

    #syntheticRegions.show()

    return syntheticRegions
