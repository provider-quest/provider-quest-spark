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
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.functions import window
from pyspark.sql.functions import last
from pyspark.sql.types import StructType, ArrayType, StringType

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("MinerPowerStaging")\
        .getOrCreate()

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
        .json('input-staging/miner-power') \
        .withWatermark("timestamp", "1 hour")

    minerPower = minerPower.withColumn(
        "date", minerPower.timestamp.astype('date'))

    schemaInfo = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("tipSet", "string") \
        .add("miner", "string") \
        .add("owner", "string") \
        .add("worker", "string") \
        .add("newWorker", "string") \
        .add("controlAddresses", ArrayType(StringType())) \
        .add("peerId", "string") \
        .add("multiaddrs", ArrayType(StringType())) \
        .add("multiaddrsDecoded", ArrayType(StringType())) \
        .add("windowPoStProofType", "short") \
        .add("sectorSize", "long") \
        .add("windowPoStPartitionSectors", "long") \
        .add("consensusFaultElapsed", "long")

    minerInfo = spark \
        .readStream \
        .schema(schemaInfo) \
        .json('input-staging/miner-info') \
        .withWatermark("timestamp", "1 hour")

    minerInfo = minerInfo.withColumn(
        "date", minerInfo.timestamp.astype('date'))

    schemaAsks = StructType() \
        .add("epoch", "long") \
        .add("timestamp", "timestamp") \
        .add("miner", "string") \
        .add("seqNo", "integer") \
        .add("askTimestamp", "long") \
        .add("price", "string") \
        .add("verifiedPrice", "string") \
        .add("minPieceSize", "long") \
        .add("maxPieceSize", "long") \
        .add("expiry", "long") \
        .add("error", "string") \
        .add("startTime", "timestamp") \
        .add("endTime", "timestamp")

    asks = spark \
        .readStream \
        .schema(schemaAsks) \
        .json('input-staging/asks') \
        .withWatermark("timestamp", "1 hour")

    asks = asks \
        .withColumn("date", asks.timestamp.astype('date')) \
        .withColumn("priceDouble", asks.price.astype('double')) \
        .withColumn("verifiedPriceDouble", asks.verifiedPrice.astype('double'))

    numberOfPowerRecords = minerPower.groupBy().count()
    numberOfInfoRecords = minerInfo.groupBy().count()
    numberOfAsksRecords = asks.groupBy().count()

    averagePowerHourly = minerPower.groupBy(
        minerPower.miner,
        minerPower.date,
        window(minerPower.timestamp, '1 hour')
    ).avg("rawBytePower", "qualityAdjPower")

    averagePowerDaily = minerPower.groupBy(
        minerPower.miner,
        minerPower.date,
        window(minerPower.timestamp, '1 day')
    ).avg("rawBytePower", "qualityAdjPower")

    averagePowerMultiDay = minerPower.groupBy(
        minerPower.miner,
        window(minerPower.timestamp, '2 day', '2 day')
    ).avg("rawBytePower", "qualityAdjPower")

    latestMinerInfoSubset = minerInfo \
        .groupBy('miner') \
        .agg(last('sectorSize'), last('peerId'), last('multiaddrsDecoded'))

    latestAsksSubset = asks \
        .groupBy('miner') \
        .agg(
            last('price'),
            last('verifiedPrice'),
            last('priceDouble'),
            last('verifiedPriceDouble'),
            last('minPieceSize'),
            last('maxPieceSize'),
            last('askTimestamp'),
            last('expiry'),
            last('seqNo'),
            last('error'))

    # query = minerPower \
    #    .writeStream \
    #    .queryName("miner_power_json") \
    #    .format("json") \
    #    .option("path", "output-staging/miner_power/json") \
    #    .option("checkpointLocation", "checkpoint-staging/miner_power/json") \
    #    .partitionBy("date", "miner") \
    #    .start()

    queryArchiveMinerInfo = minerInfo \
        .writeStream \
        .queryName("miner_info_json") \
        .format("json") \
        .option("path", "output-staging/miner_info/json") \
        .option("checkpointLocation", "checkpoint-staging/miner_info/json") \
        .partitionBy("date", "miner") \
        .start()

    queryArchiveAsks = asks \
        .writeStream \
        .queryName("asks_json") \
        .format("json") \
        .option("path", "output-staging/asks/json") \
        .option("checkpointLocation", "checkpoint-staging/asks/json") \
        .partitionBy("date", "miner") \
        .start()

    # queryLatestMinerInfoSubset = latestMinerInfoSubset \
    #    .writeStream \
    #    .queryName("miner_info_subset_latest_json") \
    #    .outputMode('complete') \
    #    .format("console") \
    #    .start()

    def output_latest_miner_info_subset(df, epoch_id):
        df.coalesce(1).write.json(
            'output-staging/miner_info/json_latest_subset', mode='overwrite')

    queryLatestMinerInfoSubset = latestMinerInfoSubset \
        .writeStream \
        .queryName("miner_info_subset_latest_json") \
        .outputMode('complete') \
        .foreachBatch(output_latest_miner_info_subset) \
        .start()

    def output_latest_asks_subset(df, epoch_id):
        df.coalesce(1).write.json(
            'output-staging/asks/json_latest_subset', mode='overwrite')

    queryLatestAsksSubset = latestAsksSubset \
        .writeStream \
        .queryName("asks_subset_latest_json") \
        .outputMode('complete') \
        .foreachBatch(output_latest_asks_subset) \
        .start()

    # def output_latest_miner_info_subset_changes(df, epoch_id):
    #    df.coalesce(1).write.json(
    #        'output-staging/miner_info/json_latest_subset_changes/' + epoch_id + '/', mode='overwrite')

    # queryLatestMinerInfoSubsetChanges = latestMinerInfoSubset \
    #    .writeStream \
    #    .queryName("miner_info_subset_latest_json_changes") \
    #    .outputMode('update') \
    #    .format("console") \
    #    .start()

    #    .foreachBatch(output_latest_miner_info_subset_changes) \

    # def output_latest_miner_info_subset2(df, epoch_id):
    #    df.coalesce(1).write.json(
    #        'output-staging/miner_info/json_latest_subset2', mode='overwrite')

    # queryLatestMinerInfoSubset = latestMinerInfoSubset \
    #    .writeStream \
    #    .queryName("miner_info_subset2_latest_json") \
    #    .outputMode('complete') \
    #    .foreachBatch(output_latest_miner_info_subset2) \
    #    .start()

    #    .option("path", "output-staging/miner_info/json_peerids") \
    #    .option("checkpointLocation", "checkpoint-staging/miner_info/json_peerids") \

    # Start running the query that prints the running counts to the console
    # query2 = numberOfRecords \
    #    .writeStream \
    #    .queryName("miner_power_counter") \
    #    .outputMode('complete') \
    #    .format('console') \
    #    .start()

    queryInfoCount = numberOfInfoRecords \
        .writeStream \
        .queryName("miner_info_counter") \
        .outputMode('complete') \
        .format('console') \
        .start()

    # query3 = averagePowerHourly \
    #    .writeStream \
    #    .queryName("miner_power_avg_hourly_json") \
    #    .format("json") \
    #    .option("path", "output-staging/miner_power/json_avg_hourly") \
    #    .option("checkpointLocation", "checkpoint-staging/miner_power/json_avg_hourly") \
    #    .partitionBy("date", "miner") \
    #    .start()

    # query4 = averagePowerDaily \
    #    .writeStream \
    #    .queryName("miner_power_avg_daily_json") \
    #    .format("json") \
    #    .option("path", "output-staging/miner_power/json_avg_daily") \
    #    .option("checkpointLocation", "checkpoint-staging/miner_power/json_avg_daily") \
    #    .partitionBy("date", "miner") \
    #    .start()

    # query5 = averagePowerMultiDay \
    #    .writeStream \
    #    .queryName("miner_power_avg_multiday_json") \
    #    .format("json") \
    #    .option("path", "output-staging/miner_power/json_avg_multiday") \
    #    .option("checkpointLocation", "checkpoint-staging/miner_power/json_avg_multiday") \
    #    .partitionBy("window", "miner") \
    #    .start()

    while True:
        for stream in spark.streams.active:
            if stream.status['message'] != "Waiting for data to arrive":
                print(stream.name, stream.status['message'])
        time.sleep(1)
