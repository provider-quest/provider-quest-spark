#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

r"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network.
 Usage: structured_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Structured Streaming
   would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py
    localhost 9999`
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import StructType

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerPower")\
        .getOrCreate()

    schema = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("tipSet", "string") \
        .add("miner", "string") \
        .add("rawBytePower", "double") \
        .add("qualityAdjPower", "double")

    # Create DataFrame representing the stream of input lines from connection to host:port
    minerPower = spark \
        .readStream \
        .schema(schema) \
        .json('input') \
        .withWatermark("timestamp", "10 minutes")

    # Generate running word count
    # wordCounts = words.groupBy('word').count()

    numberOfRecords = minerPower.groupBy().count()

    averagePowerHourly = minerPower.groupBy(
            minerPower.miner,
            window(minerPower.timestamp, '1 hour')
    ).avg("rawBytePower", "qualityAdjPower")

    # Start running the query that prints the running counts to the console
    #def output_counts(df, epoch_id):
    #    df.coalesce(1).write.csv('output/word_counts', mode='overwrite')
    #    pass

    #query = minerPower\
    #    .writeStream\
    #    .outputMode('complete')\
    #    .foreachBatch(output_counts)\
    #    .start()

    query = minerPower \
        .writeStream \
        .format("json") \
        .option("path", "output/json") \
        .option("checkpointLocation", "checkpoint/json") \
        .partitionBy("miner") \
        .start()

    # Start running the query that prints the running counts to the console
    query2 = numberOfRecords \
        .writeStream \
        .outputMode('complete') \
        .format('console') \
        .start()

    query3 = averagePowerHourly \
        .writeStream \
        .format("json") \
        .option("path", "output/json_avg_power_hourly") \
        .option("checkpointLocation", "checkpoint/json_avg_power_hourly") \
        .partitionBy("miner") \
        .start()

    query4 = minerPower \
        .writeStream \
        .format("parquet") \
        .option("mode", "append") \
        .option("path", "output/parquet") \
        .option("checkpointLocation", "checkpoint/parquet") \
        .partitionBy("miner") \
        .start()


    query.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
    query4.awaitTermination()
