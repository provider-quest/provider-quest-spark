import sys
import time

from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct
from pyspark.sql.functions import hour
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StructType, ArrayType, StringType


def process(multiaddrsIps, ipsGeoLite2, suffix=""):

    inputDir = 'input' + suffix
    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    multiaddrsGeoLite2 = multiaddrsIps.join(
      ipsGeoLite2,
      multiaddrsIps.ip == ipsGeoLite2.ip
    )

    queryArchiveMultiaddrsGeoLite2 = multiaddrsGeoLite2 \
      .writeStream \
      .queryName("multiaddrs_geolite2_json") \
      .format("json") \
      .option("path", outputDir + "/multiaddrs_geolite2/json") \
      .option("checkpointLocation", checkpointDir + "/multiaddrs_geolite2/json") \
      .partitionBy("miner", "date") \
      .trigger(processingTime='1 minute') \
      .start()
     