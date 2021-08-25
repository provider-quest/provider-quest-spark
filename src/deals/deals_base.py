from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct


def process(deals, suffix=""):

    outputDir = '../work/output' + suffix
    checkpointDir = '../work/checkpoint' + suffix

    # Archive

    queryArchiveDealsHourly = deals \
        .writeStream \
        .queryName("deals_archive_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals/archive/hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals/archive/hourly/json") \
        .partitionBy("date", "hour") \
        .trigger(processingTime='1 minute') \
        .start()

    # Aggregate Hourly

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

    queryAggrDealsHourly = dealsHourlyAggr \
        .writeStream \
        .queryName("deals_aggr_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals/aggr_hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals/aggr_hourly/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

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

    queryAggrDealsHourlyByVerified = dealsHourlyAggrByVerified \
        .writeStream \
        .queryName("deals_by_verified_aggr_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_verified/aggr_hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_verified/aggr_hourly/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    # Aggregate Daily

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

    queryAggrDealsDaily = dealsDailyAggr \
        .writeStream \
        .queryName("deals_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/aggr_daily/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

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

    queryAggrDealsDailyByVerified = dealsDailyAggrByVerified \
        .writeStream \
        .queryName("deals_by_verified_aggr_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_verified/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_verified/aggr_daily/json") \
        .partitionBy("date") \
        .trigger(processingTime='1 minute') \
        .start()

    # Aggregate Weekly

    dealsWeeklyAggr = deals.groupBy(
        window(deals.messageTime, '7 days')
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
    ).withColumn('windowStart', expr('window.start').astype('date'))

    queryAggrDealsWeekly = dealsWeeklyAggr \
        .writeStream \
        .queryName("deals_aggr_weekly_json") \
        .format("json") \
        .option("path", outputDir + "/deals/aggr_weekly/json") \
        .option("checkpointLocation", checkpointDir + "/deals/aggr_weekly/json") \
        .partitionBy("windowStart") \
        .trigger(processingTime='1 minute') \
        .start()

    dealsWeeklyAggrByVerified = deals.groupBy(
        window(deals.messageTime, '7 days'),
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
    ).withColumn('windowStart', expr('window.start').astype('date'))

    queryAggrDealsWeeklyByVerified = dealsWeeklyAggrByVerified \
        .writeStream \
        .queryName("deals_by_verified_aggr_weekly_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_verified/aggr_weekly/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_verified/aggr_weekly/json") \
        .partitionBy("windowStart") \
        .trigger(processingTime='1 minute') \
        .start()
