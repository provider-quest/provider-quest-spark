import os
from pyspark.sql.types import StructType

def get(spark, suffix=""):

    base_dir = os.environ.get('WORK_DIR') or '.'

    input_dir = base_dir + '/' + 'input' + suffix

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
        .json(input_dir + '/miner-power') \
        .withWatermark("timestamp", "1 minute")

    minerPower = minerPower.withColumn(
        "date", minerPower.timestamp.astype('date'))

    return minerPower
