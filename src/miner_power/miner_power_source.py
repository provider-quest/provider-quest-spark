from pyspark.sql.types import StructType

def get(spark, suffix=""):

    inputDir = 'input' + suffix

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

    return minerPower