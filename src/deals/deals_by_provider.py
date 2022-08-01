import os
from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct


def process(deals, suffix=""):

    outputDir = os.environ.get('OUTPUT_DEALS_DIR') or base_dir + '/output' + suffix
    checkpointDir = os.environ.get('CHECKPOINT_DEALS_DIR') or base_dir + '/checkpoint' + suffix

    # Archive

    """
    queryArchiveDealsByProvider = deals \
        .drop("hour") \
        .writeStream \
        .queryName("deals_by_provider_archive_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_provider/archive/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_provider/archive/json") \
        .partitionBy("date", "provider") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    # By Provider - Aggregated Daily

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

    queryAggrDealsDailyByProvider = dealsDailyAggrByProvider \
        .writeStream \
        .queryName("deals_by_provider_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_provider/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_provider/aggr_daily/json") \
        .partitionBy("date", "provider") \
        .trigger(processingTime='1 minute') \
        .start()

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

    queryAggrDealsDailyByProviderVerified = dealsDailyAggrByProviderVerified \
        .writeStream \
        .queryName("deals_by_provider_by_verified_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_provider/by_verified/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_provider/by_verified/aggr_daily/json") \
        .partitionBy("date", "provider") \
        .trigger(processingTime='1 minute') \
        .start()

    dealsDailyAggrByProviderVerifiedFlat = dealsDailyAggrByProviderVerified.drop('window')

    queryAggrDealsDailyByProviderVerifiedCsv = dealsDailyAggrByProviderVerifiedFlat \
        .writeStream \
        .queryName("deals_by_provider_by_verified_aggr_daily_csv") \
        .format("csv") \
        .option("path", outputDir + "/deals/by_provider/by_verified/aggr_daily/csv") \
        .option("checkpointLocation", checkpointDir + "/deals/by_provider/by_verified/aggr_daily/csv") \
        .option("header", True) \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    # Multiday - By Provider

    dealsMultidayAggrByProvider = deals.groupBy(
        window(deals.messageTime, '7 days', '1 day'),
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

    queryAggrDealsMultidayByProvider = dealsMultidayAggrByProvider \
        .writeStream \
        .queryName("deals_by_provider_aggr_multiday_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_provider/aggr_multiday/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_provider/aggr_multiday/json") \
        .partitionBy("window") \
        .trigger(processingTime='1 minute') \
        .start()
