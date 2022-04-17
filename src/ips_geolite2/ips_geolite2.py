import os
import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, MapType, StringType

def process(spark, suffix=""):

    inputDir = os.environ.get('INPUT_IPS_GEOLITE2_DIR') or \
             'input' + suffix + '/ips-geolite2'
    outputDir = os.environ.get('OUTPUT_IPS_GEOLITE2_DIR') or \
            '../work/output' + suffix + '/ips-geolite2'
    checkpointDir = os.environ.get('CHECKPOINT_IPS_GEOLITE2_DIR') or \
            '../work/checkpoint' + suffix + '/ips-geolite2'

    schemaIpsGeoLite2 = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("ip", "string") \
        .add("continent", "string") \
        .add("country", "string") \
        .add("subdiv1", "string") \
        .add("city", "string") \
        .add("long", "float") \
        .add("lat", "float") \
        .add("geolite2", "string")

    ipsGeoLite2 = spark \
        .readStream \
        .schema(schemaIpsGeoLite2) \
        .json(inputDir) \
        .withWatermark("timestamp", "1 minute")

    ipsGeoLite2 = ipsGeoLite2.withColumn(
        "date", ipsGeoLite2.timestamp.astype('date'))

    queryArchiveIpsGeoLite2 = ipsGeoLite2 \
        .writeStream \
        .queryName("ips_geolite2_json") \
        .format("json") \
        .option("path", outputDir + "/json") \
        .option("checkpointLocation", checkpointDir + "/json") \
        .partitionBy("ip", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    latestIpsGeoLite2 = ipsGeoLite2 \
        .groupBy(
          'ip'
        ).agg(
          last('epoch'),
          last('timestamp'),
          last('continent'),
          last('country'),
          last('subdiv1'),
          last('city'),
          last('long'),
          last('lat'),
          last('geolite2')
        )

    def output_latest_ips_geolite2(df, epoch_id):
        df.coalesce(1).write.json(
            outputDir + '/json_latest', mode='overwrite')

    queryLatestIpsGeoLite2 = latestIpsGeoLite2 \
        .writeStream \
        .queryName("ips_geolite2_latest_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/json_latest") \
        .foreachBatch(output_latest_ips_geolite2) \
        .trigger(processingTime='1 minute') \
        .start()

    return ipsGeoLite2

