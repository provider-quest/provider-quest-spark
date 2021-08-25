from pyspark.sql.functions import window
from pyspark.sql.functions import count
from pyspark.sql.functions import sum
from pyspark.sql.functions import last
from pyspark.sql.functions import min, max, avg
from pyspark.sql.functions import when
from pyspark.sql.functions import expr
from pyspark.sql.functions import approx_count_distinct


def process(deals, minerRegions, suffix=""):

    outputDir = '../work/output' + suffix
    checkpointDir = '../work/checkpoint' + suffix

    dealsWithRegions = deals \
        .join(
            minerRegions,
            deals.provider == minerRegions.miner,
            how='leftOuter'
        ) \
        .fillna('none', 'region') \
        .drop('miner')

    dealsWithRegions = dealsWithRegions \
        .withColumn("splitPieceSizeDouble",
                    dealsWithRegions.pieceSizeDouble /
                    dealsWithRegions.numRegions) \
        .withColumn("splitLifetimeValue",
                    dealsWithRegions.lifetimeValue /
                    dealsWithRegions.numRegions)

    # Archive

    queryArchiveDealsByRegion = dealsWithRegions \
        .drop("hour", "clientProvider") \
        .writeStream \
        .queryName("deals_by_miner_region_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_miner_region/archive/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_miner_region/archive/json") \
        .partitionBy("region", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    # Aggregate Daily

    dealsDailyAggrByRegionByProvider = dealsWithRegions.groupBy(
        dealsWithRegions.provider,
        dealsWithRegions.date,
        dealsWithRegions.region,
        window('messageTime', '1 day'),
    ).agg(
        expr("count(*) as count"),
        sum(dealsWithRegions.pieceSizeDouble),
        sum(dealsWithRegions.splitPieceSizeDouble),
        sum(dealsWithRegions.lifetimeValue),
        sum(dealsWithRegions.splitLifetimeValue)
    )

    dealsDailyAggrByRegionByProvider = dealsDailyAggrByRegionByProvider \
        .withColumn(
            'pieceSizeDouble',
            when(
                dealsWithRegions.region == 'none',
                dealsDailyAggrByRegionByProvider['sum(pieceSizeDouble)']
            ).otherwise(dealsDailyAggrByRegionByProvider['sum(splitPieceSizeDouble)'])
        ) \
        .withColumn(
            'lifetimeValue',
            when(
                dealsWithRegions.region == 'none',
                dealsDailyAggrByRegionByProvider['sum(lifetimeValue)']
            ).otherwise(dealsDailyAggrByRegionByProvider['sum(splitLifetimeValue)'])
        )

    queryAggrDealsByRegionByProviderDaily = dealsDailyAggrByRegionByProvider \
        .writeStream \
        .queryName("deals_aggr_by_miner_region_by_provider_daily_with_splits_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_miner_region/by_provider/aggr_daily_with_splits/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_miner_region/by_provider/aggr_daily_with_splits/json") \
        .partitionBy("region", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    def output_summed(df, epoch_id):
        summedDf = df.groupBy(
            'date',
            'region'
        ).agg(
            sum('count').alias('count'),
            count('provider'),
            sum('pieceSizeDouble'),
            sum('lifetimeValue'),
        )

        # summedDf.coalesce(1).write.partitionBy('date').json(
        summedDf.orderBy('date', 'region').coalesce(1).write.json(
            outputDir + '/deals/by_miner_region/sum_aggr_daily/json',
            mode='overwrite')

    queryDealsSumAvgDaily = dealsDailyAggrByRegionByProvider \
        .writeStream \
        .queryName("deals_by_miner_region_sum_aggr_daily_json") \
        .outputMode('complete') \
        .option("checkpointLocation", checkpointDir + "/deals/by_miner_region/sum_aggr_daily/json") \
        .foreachBatch(output_summed) \
        .trigger(processingTime='1 minute') \
        .start()

    dealsDailyAggrByRegion = dealsWithRegions.groupBy(
        dealsWithRegions.date,
        dealsWithRegions.region,
        window('messageTime', '1 day'),
    ).agg(
        expr("count(*) as count"),
        min(dealsWithRegions.storagePricePerEpochDouble),
        max(dealsWithRegions.storagePricePerEpochDouble),
        avg(dealsWithRegions.storagePricePerEpochDouble),
        approx_count_distinct(dealsWithRegions.label),
        approx_count_distinct(dealsWithRegions.provider),
        approx_count_distinct(dealsWithRegions.client),
        approx_count_distinct(dealsWithRegions.clientProvider)
    )

    queryAggrDealsByRegionDaily = dealsDailyAggrByRegion \
        .writeStream \
        .queryName("deals_aggr_by_miner_region_daily_json") \
        .format("json") \
        .option("path", outputDir + "/deals/by_miner_region/aggr_daily/json") \
        .option("checkpointLocation", checkpointDir + "/deals/by_miner_region/aggr_daily/json") \
        .trigger(processingTime='1 minute') \
        .start()

    # Aggregate Hourly

