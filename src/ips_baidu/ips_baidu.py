import os
import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, MapType, StringType

def process(spark, suffix=""):

    inputDir = os.environ.get('INPUT_IPS_BAIDU_DIR') or \
             'input' + suffix + '/ips-baidu'
    outputDir = os.environ.get('OUTPUT_IPS_BAIDU_DIR') or \
            '../work/output' + suffix + '/ips-baidu'
    checkpointDir = os.environ.get('CHECKPOINT_IPS_BAIDU_DIR') or \
            '../work/checkpoint' + suffix + '/ips-baidu'

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
        .json(inputDir) \
        .withWatermark("timestamp", "1 minute")

    ipsBaidu = ipsBaidu.withColumn(
        "date", ipsBaidu.timestamp.astype('date'))

    queryArchiveIpsBaidu = ipsBaidu \
        .writeStream \
        .queryName("ips_baidu_json") \
        .format("json") \
        .option("path", outputDir + "/json") \
        .option("checkpointLocation", checkpointDir + "/json") \
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
            outputDir + '/json_latest', mode='overwrite')

    queryLatestIpsBaidu = latestIpsBaidu \
        .writeStream \
        .queryName("ips_baidu_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/json_latest") \
        .foreachBatch(output_latest_ips_baidu) \
        .trigger(processingTime='1 minute') \
        .start()

    return ipsBaidu

