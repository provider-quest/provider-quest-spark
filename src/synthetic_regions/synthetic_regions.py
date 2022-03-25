import os
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType

def get_latest(spark, suffix=""):

    inputDir = os.environ.get('INPUT_SYNTHETIC_REGIONS_DIR') or \
            'input' + suffix + '/synthetic-regions'

    candidates = os.listdir(inputDir)
    epoch = sorted([int(n) for n in candidates])[-1]

    schemaSyntheticRegions = StructType() \
        .add("provider", "string") \
        .add("region", "string") \
        .add("numRegions", "short")

    syntheticRegions = spark \
        .read \
        .schema(schemaSyntheticRegions) \
        .json(f"{inputDir}/{epoch}/synthetic-provider-regions-{epoch}.json")

    syntheticRegions = syntheticRegions.withColumn("syntheticRegionEpoch", lit(epoch))

    #syntheticRegions.show()

    return syntheticRegions
