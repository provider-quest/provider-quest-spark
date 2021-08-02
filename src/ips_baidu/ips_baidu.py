import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, MapType, StringType

def process(spark, suffix=""):

    inputDir = 'input' + suffix
    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    schemaIpsBaidu = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("ip", "string") \
        .add("city", "string") \
        .add("long", "float") \
        .add("lat", "float") \
        .add("baidu", "string")

    ipsBaidu = spark \
        .readStream \
        .schema(schemaIpsBaidu) \
        .json(inputDir + '/ips-baidu') \
        .withWatermark("timestamp", "1 minute")

    ipsBaidu = ipsBaidu.withColumn(
        "date", ipsBaidu.timestamp.astype('date'))

    queryArchiveIpsBaidu = ipsBaidu \
        .writeStream \
        .queryName("ips_baidu_json") \
        .format("json") \
        .option("path", outputDir + "/ips_baidu/json") \
        .option("checkpointLocation", checkpointDir + "/ips_baidu/json") \
        .partitionBy("ip", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    latestIpsBaidu = ipsBaidu \
        .groupBy(
          'ip'
        ).agg(
          last('epoch'),
          last('timestamp'),
          last('city'),
          last('long'),
          last('lat'),
          last('baidu')
        )

    def output_latest_ips_baidu(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/ips_baidu/json_latest', mode='overwrite')

    queryLatestIpsBaidu = latestIpsBaidu \
        .writeStream \
        .queryName("ips_baidu_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/ips_baidu/json_latest") \
        .foreachBatch(output_latest_ips_baidu) \
        .trigger(processingTime='1 minute') \
        .start()

    return ipsBaidu

