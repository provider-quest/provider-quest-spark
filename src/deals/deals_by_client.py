from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct


def process(deals, suffix=""):

    outputDir = '../work/output' + suffix
    checkpointDir = '../work/checkpoint' + suffix

    # Archive

    queryArchiveDealsByClient = deals \
        .drop("hour") \
        .writeStream \
        .queryName("deals_by_client_archive_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_client/archive/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_client/archive/json") \
        .partitionBy("date", "client") \
        .trigger(processingTime='1 minute') \
        .start()

    # By Client - Aggregated Daily

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

    queryAggrDealsDailyByClient = dealsDailyAggrByClient \
        .writeStream \
        .queryName("deals_by_client_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_client/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_client/aggr_daily/json") \
        .partitionBy("date", "client") \
        .trigger(processingTime='1 minute') \
        .start()

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

    queryAggrDealsDailyByClientVerified = dealsDailyAggrByClientVerified \
        .writeStream \
        .queryName("deals_by_client_by_verified_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_client/by_verified/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_client/by_verified/aggr_daily/json") \
        .partitionBy("date", "client") \
        .trigger(processingTime='1 minute') \
        .start()

