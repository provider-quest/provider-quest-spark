from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct


def process(deals, client_names, suffix=""):

    outputDir = '../work/output' + suffix
    checkpointDir = '../work/checkpoint' + suffix

    dealsWithClientNames = deals.join(client_names, client_names.address == deals.client)

    # Archive

    """
    queryArchiveDealsByClientName = dealsWithClientNames \
        .drop("hour", "address", "clientProvider") \
        .writeStream \
        .queryName("deals_by_client_name_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_client_name/archive/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_client_name/archive/json") \
        .partitionBy("date", "clientName") \
        .trigger(processingTime='1 minute') \
        .start()
    """

    # Aggregate Daily

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

    queryAggrDealsByClientNameDaily = dealsDailyAggrByClientName \
        .writeStream \
        .queryName("deals_aggr_by_client_name_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_client_name/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_client_name/aggr_daily/json") \
        .partitionBy("clientName", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    # Aggregate Hourly

    dealsHourlyAggrByClientName = dealsWithClientNames.groupBy(
        dealsWithClientNames.date,
        dealsWithClientNames.hour,
        window(dealsWithClientNames.messageTime, '1 hour'),
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

    queryAggrDealsByClientNameHourly = dealsHourlyAggrByClientName \
        .writeStream \
        .queryName("deals_aggr_by_client_name_hourly_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_client_name/aggr_hourly/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_client_name/aggr_hourly/json") \
        .partitionBy("clientName", "date") \
        .trigger(processingTime='1 minute') \
        .start()

