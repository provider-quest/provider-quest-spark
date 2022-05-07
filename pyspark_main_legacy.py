import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, StringType

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from asks import asks

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerReport")\
        .getOrCreate()

    suffix = ''

    asks.process_asks(spark, suffix)

    while True:
        for stream in spark.streams.active:
            message = stream.status['message']
            if message != "Waiting for data to arrive" and \
                    message != "Waiting for next trigger" and \
                    message.find("Getting offsets") == -1:
                print(stream.name, message)
        time.sleep(10)
