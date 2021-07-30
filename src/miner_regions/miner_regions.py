import os
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType

def get_latest(spark, suffix=""):

    inputDir = 'input' + suffix

    candidates = os.listdir(inputDir + '/miner-regions')
    epoch = sorted([int(n) for n in candidates])[-1]

    schemaMinerRegions = StructType() \
        .add("miner", "string") \
        .add("region", "string") \
        .add("numRegions", "short")

    minerRegions = spark \
        .read \
        .schema(schemaMinerRegions) \
        .json(f"{inputDir}/miner-regions/{epoch}/miner-regions-{epoch}.json")

    minerRegions = minerRegions.withColumn("minerRegionEpoch", lit(epoch))

    #minerRegions.show()

    return minerRegions