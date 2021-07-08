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


def process_deals(spark, client_names, suffix=""):

    inputDir = 'input' + suffix
    outputDir = 'output' + suffix
    checkpointDir = 'checkpoint' + suffix

    schemaDeals = StructType() \
        .add("dealId", "long") \
        .add("messageHeight", "long") \
        .add("messageTime", "timestamp") \
        .add("messageCid", "string") \
        .add("pieceCid", "string") \
        .add("pieceSize", "long") \
        .add("verifiedDeal", "boolean") \
        .add("client", "string") \
        .add("provider", "string") \
        .add("label", "string") \
        .add("startEpoch", "long") \
        .add("startTime", "timestamp") \
        .add("endEpoch", "long") \
        .add("endTime", "timestamp") \
        .add("storagePricePerEpoch", "string") \
        .add("providerCollateral", "string") \
        .add("clientCollateral", "string")

    deals = spark \
        .readStream \
        .schema(schemaDeals) \
        .json(inputDir + '/deals') \
        .withWatermark("messageTime", "1 minute")

    deals = deals \
        .withColumn("date", deals.messageTime.astype('date')) \
        .withColumn("clientProvider", concat_ws('-', deals.client, deals.provider)) \
        .withColumn("storagePricePerEpochDouble", deals.storagePricePerEpoch.astype('double')) \
        .withColumn("providerCollateralDouble", deals.providerCollateral.astype('double')) \
        .withColumn("clientCollateralDouble", deals.clientCollateral.astype('double')) \
        .withColumn("pieceSizeDouble", deals.pieceSize.astype('double')) \
        .withColumn("lifetimeValue",
                    expr("storagePricePerEpochDouble * (endEpoch - startEpoch) * " +
                         "pieceSize / 1e18 / 1024 / 1024 / 1024")) 

    dealsHourly = deals \
        .withColumn("hour", hour(deals.messageTime))

    dealsWithClientNames = deals.join(client_names, client_names.address == deals.client)

    dealsWithClientNamesHourly = dealsWithClientNames \
        .withColumn("hour", hour(deals.messageTime))


    dealsHourlyAggr = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 hour')
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsHourlyAggrByVerified = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 hour'),
        deals.verifiedDeal
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggr = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day')
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggrByVerified = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.verifiedDeal
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggrByProvider = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.provider
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggrByClient = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.client
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggrByClientName = dealsWithClientNames.groupBy(
        dealsWithClientNames.date,
        window(dealsWithClientNames.messageTime, '1 day'),
        dealsWithClientNames.clientName
    ).agg(
        expr("count(*) as count"),
        sum(dealsWithClientNames.pieceSizeDouble),
        avg(dealsWithClientNames.pieceSizeDouble),
        min(dealsWithClientNames.pieceSizeDouble),
        max(dealsWithClientNames.pieceSizeDouble),
        avg(dealsWithClientNames.storagePricePerEpochDouble),
        min(dealsWithClientNames.storagePricePerEpochDouble),
        max(dealsWithClientNames.storagePricePerEpochDouble),
        approx_count_distinct(dealsWithClientNames.label),
        sum(dealsWithClientNames.lifetimeValue),
        avg(dealsWithClientNames.lifetimeValue),
        min(dealsWithClientNames.lifetimeValue),
        max(dealsWithClientNames.lifetimeValue),
        approx_count_distinct(dealsWithClientNames.provider),
        approx_count_distinct(dealsWithClientNames.client),
        approx_count_distinct(dealsWithClientNames.clientProvider)
    )

    dealsHourlyAggrByClientName = dealsWithClientNamesHourly.groupBy(
        dealsWithClientNames.date,
        dealsWithClientNamesHourly.hour,
        window(dealsWithClientNames.messageTime, '1 day'),
        dealsWithClientNames.clientName
    ).agg(
        expr("count(*) as count"),
        sum(dealsWithClientNames.pieceSizeDouble),
        avg(dealsWithClientNames.pieceSizeDouble),
        min(dealsWithClientNames.pieceSizeDouble),
        max(dealsWithClientNames.pieceSizeDouble),
        avg(dealsWithClientNames.storagePricePerEpochDouble),
        min(dealsWithClientNames.storagePricePerEpochDouble),
        max(dealsWithClientNames.storagePricePerEpochDouble),
        approx_count_distinct(dealsWithClientNames.label),
        sum(dealsWithClientNames.lifetimeValue),
        avg(dealsWithClientNames.lifetimeValue),
        min(dealsWithClientNames.lifetimeValue),
        max(dealsWithClientNames.lifetimeValue),
        approx_count_distinct(dealsWithClientNames.provider),
        approx_count_distinct(dealsWithClientNames.client),
        approx_count_distinct(dealsWithClientNames.clientProvider)
    )

    dealsDailyAggrByPairs = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.clientProvider
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggrByProviderVerified = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.provider,
        deals.verifiedDeal
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggrByClientVerified = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.client,
        deals.verifiedDeal
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsDailyAggrByPairsVerified = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.clientProvider,
        deals.verifiedDeal
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsHourlyAggrByPairsVerified = dealsHourly.groupBy(
        dealsHourly.date,
        dealsHourly.hour,
        window(dealsHourly.messageTime, '1 hour'),
        dealsHourly.clientProvider,
        dealsHourly.verifiedDeal
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.provider),
        approx_count_distinct(deals.client),
        approx_count_distinct(deals.clientProvider)
    )

    dealsMultidayAggrByProvider = deals.groupBy(
        window(deals.messageTime, '2 days', '2 days'),
        deals.provider
    ).agg(
        expr("count(*) as count"),
        sum(deals.pieceSizeDouble),
        avg(deals.pieceSizeDouble),
        min(deals.pieceSizeDouble),
        max(deals.pieceSizeDouble),
        avg(deals.storagePricePerEpochDouble),
        min(deals.storagePricePerEpochDouble),
        max(deals.storagePricePerEpochDouble),
        approx_count_distinct(deals.label),
        sum(deals.lifetimeValue),
        avg(deals.lifetimeValue),
        min(deals.lifetimeValue),
        max(deals.lifetimeValue),
        approx_count_distinct(deals.client)
    )

    queryArchiveDealsByProvider = deals \
        .writeStream \
        .queryName("deals_by_provider_json") \
        .format("json") \
        .option("path", outputDir + "/deals_by_provider/json") \
        .option("checkpointLocation", checkpointDir + "/deals_by_provider/json") \
        .partitionBy("date", "provider") \
        .trigger(processingTime='1 minute') \
        .start()

    queryArchiveDealsByClient = deals \
        .writeStream \
        .queryName("deals_by_client_json") \
        .format("json") \
        .option("path", outputDir + "/deals_by_client/json") \
        .option("checkpointLocation", checkpointDir + "/deals_by_client/json") \
        .partitionBy("date", "client") \
        .trigger(processingTime='1 minute') \
        .start()

    queryArchiveDealsHourly = dealsHourly \
        .writeStream \
        .queryName("deals_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals_hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals_hourly/json") \
        .partitionBy("date", "hour") \
        .trigger(processingTime='1 minute') \
        .start()

    queryArchiveDealsByClientName = dealsWithClientNames \
        .writeStream \
        .queryName("deals_by_client_name_json") \
        .format("json") \
        .option("path", outputDir + "/deals_by_client_name/json") \
        .option("checkpointLocation", checkpointDir + "/deals_by_client_name/json") \
        .partitionBy("date", "clientName") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsHourly = dealsHourlyAggr \
        .writeStream \
        .queryName("deals_aggr_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_hourly/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsHourlyByVerified = dealsHourlyAggrByVerified \
        .writeStream \
        .queryName("deals_aggr_hourly_by_verified_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_hourly_by_verified/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_hourly_by_verified/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDaily = dealsDailyAggr \
        .writeStream \
        .queryName("deals_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDailyByVerified = dealsDailyAggrByVerified \
        .writeStream \
        .queryName("deals_aggr_daily_by_verified_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily_by_verified/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily_by_verified/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDailyByProvider = dealsDailyAggrByProvider \
        .writeStream \
        .queryName("deals_aggr_daily_by_provider_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily_by_provider/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily_by_provider/json") \
        .partitionBy("date", "provider") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDailyByClient = dealsDailyAggrByClient \
        .writeStream \
        .queryName("deals_aggr_daily_by_client_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily_by_client/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily_by_client/json") \
        .partitionBy("date", "client") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsByClientNameDaily = dealsDailyAggrByClientName \
        .writeStream \
        .queryName("deals_aggr_by_client_name_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_by_client_name_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_by_client_name_daily/json") \
        .partitionBy("clientName", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsByClientNameHourly = dealsHourlyAggrByClientName \
        .writeStream \
        .queryName("deals_aggr_by_client_name_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_by_client_name_hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_by_client_name_hourly/json") \
        .partitionBy("clientName", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDailyByPairs = dealsDailyAggrByPairs \
        .writeStream \
        .queryName("deals_aggr_daily_by_pairs_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily_by_pairs/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily_by_pairs/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDailyByProviderVerified = dealsDailyAggrByProviderVerified \
        .writeStream \
        .queryName("deals_aggr_daily_by_provider_verified_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily_by_provider_verified/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily_by_provider_verified/json") \
        .partitionBy("date", "provider") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDailyByClientVerified = dealsDailyAggrByClientVerified \
        .writeStream \
        .queryName("deals_aggr_daily_by_client_verified_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily_by_client_verified/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily_by_client_verified/json") \
        .partitionBy("date", "client") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsDailyByPairsVerified = dealsDailyAggrByPairsVerified \
        .writeStream \
        .queryName("deals_aggr_daily_by_pairs_verified_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_daily_by_pairs_verified/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_daily_by_pairs_verified/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    queryAggrDealsHourlyByPairsVerified = dealsHourlyAggrByPairsVerified \
        .writeStream \
        .queryName("deals_aggr_hourly_by_pairs_verified_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_hourly_by_pairs_verified/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_hourly_by_pairs_verified/json") \
        .partitionBy("date", "hour") \
        .trigger(processingTime='1 minute') \
        .start()
 
    queryAggrDealsMultidayByProvider = dealsMultidayAggrByProvider \
        .writeStream \
        .queryName("deals_aggr_multiday_by_provider_json") \
        .format("json") \
        .option("path", outputDir + "/deals_aggr_multiday_by_provider/json") \
        .option("checkpointLocation", checkpointDir + "/deals_aggr_multiday_by_provider/json") \
        .partitionBy("window") \
        .trigger(processingTime='1 minute') \
        .start()

    dealsDailyByDealId = deals.groupBy(
        deals.date,
        window(deals.messageTime, '1 day'),
        deals.dealId
    ).agg(
        last(deals.pieceCid).alias('pieceCid'),
        last(deals.pieceSize).alias('pieceSize'),
        last(deals.provider).alias('provider'),
        last(deals.label).alias('label'),
        last(deals.startEpoch).alias('startEpoch'),
        last(deals.startTime).alias('startTime'),
        last(deals.endEpoch).alias('endEpoch'),
        last(deals.endTime).alias('endTime')
    )

    def output_sample_deals_by_piece_size(df, batchId):
        df.coalesce(1).write.json(
            'output/deals/sample_by_piece_size_json/' + batchId, mode='overwrite')

    querySampleDealsByPieceSize = dealsDailyByDealId \
        .writeStream \
        .queryName("deals_sample_by_piece_size_json") \
        .format("json") \
        .option("path", outputDir + "/deals_sample_by_piece_size/json") \
        .option("checkpointLocation", checkpointDir + "/deals_sample_by_piece_size/json") \
        .partitionBy("pieceSize", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    querySampleDealsByProviderPieceSize = dealsDailyByDealId \
        .writeStream \
        .queryName("deals_sample_by_provider_piece_size_json") \
        .format("json") \
        .option("path", outputDir + "/deals_sample_by_provider_piece_size/json") \
        .option("checkpointLocation", checkpointDir + "/deals_sample_by_provider_piece_size/json") \
        .partitionBy("provider", "pieceSize", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    querySampleDealsByLabelProvider = dealsDailyByDealId \
        .writeStream \
        .queryName("deals_sample_by_label_provider_json") \
        .format("json") \
        .option("path", outputDir + "/deals_sample_by_label_provider/json") \
        .option("checkpointLocation", checkpointDir + "/deals_sample_by_label_provider/json") \
        .partitionBy("label", "provider", "date") \
        .trigger(processingTime='1 minute') \
        .start()

#        .foreachBatch(output_sample_deals_by_piece_size) \
#        .option("path", outputDir + "/deals_sample_by_piece_size/json") \
