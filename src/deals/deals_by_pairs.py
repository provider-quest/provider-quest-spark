from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct


def process(deals, suffix=""):

    outputDir = '../work/output' + suffix
    checkpointDir = '../work/checkpoint' + suffix

    # By Pairs - Aggregated Hourly

    dealsHourlyAggrByPairsVerified = deals.groupBy(
        deals.date,
        deals.hour,
        window(deals.messageTime, '1 hour'),
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

    queryAggrDealsHourlyByPairsVerified = dealsHourlyAggrByPairsVerified \
        .writeStream \
        .queryName("deals_by_pairs_by_verified_aggr_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_pairs/by_verified/aggr_hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_pairs/by_verified/aggr_hourly/json") \
        .partitionBy("date", "hour") \
        .trigger(processingTime='1 minute') \
        .start()

    # By Pairs - Aggregated Daily

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

    queryAggrDealsDailyByPairs = dealsDailyAggrByPairs \
        .writeStream \
        .queryName("deals_by_pairs_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_pairs/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_pairs/aggr_daily/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

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

    queryAggrDealsDailyByPairsVerified = dealsDailyAggrByPairsVerified \
        .writeStream \
        .queryName("deals_by_pairs_by_verified_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_pairs/by_verified/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_pairs/by_verified/aggr_daily/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

