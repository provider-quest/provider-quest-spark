import os
from pyspark.sql.functions import window
from pyspark.sql.functions import expr
from pyspark.sql.functions import last
from pyspark.sql.functions import avg
from pyspark.sql.functions import min, max, sum, approx_count_distinct


def process(deals, suffix=""):

    outputDir = os.environ.get('OUTPUT_DEALS_DIR') or base_dir + '/output' + suffix
    checkpointDir = os.environ.get('CHECKPOINT_DEALS_DIR') or base_dir + '/checkpoint' + suffix

    ### Sample deals

    """
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

    querySampleDealsByPieceSize = dealsDailyByDealId \
        .writeStream \
        .queryName("deals_sample_by_piece_size_json") \
        .format("json") \
        .option("path", outputDir + "/deals/sample/by_piece_size/json") \
        .option("checkpointLocation", checkpointDir + "/deals/sample/by_piece_size/json") \
        .partitionBy("pieceSize", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    querySampleDealsByProviderPieceSize = dealsDailyByDealId \
        .writeStream \
        .queryName("deals_sample_by_provider_by_piece_size_json") \
        .format("json") \
        .option("path", outputDir + "/deals/sample/by_provider/by_piece_size/json") \
        .option("checkpointLocation", checkpointDir + "/deals/sample/by_provider/by_piece_size/json") \
        .partitionBy("provider", "pieceSize", "date") \
        .trigger(processingTime='1 minute') \
        .start()

    querySampleDealsByLabelProvider = dealsDailyByDealId \
        .writeStream \
        .queryName("deals_sample_by_label_by_provider_json") \
        .format("json") \
        .option("path", outputDir + "/deals/sample/by_label/by_provider/json") \
        .option("checkpointLocation", checkpointDir + "/deals/sample/by_label/by_provider/json") \
        .partitionBy("label", "provider", "date") \
        .trigger(processingTime='1 minute') \
        .start()
    """
