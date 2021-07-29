from pyspark.sql.functions import expr
from pyspark.sql.functions import hour
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StructType


def get(spark, suffix=""):

    inputDir = 'input' + suffix

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
        .withColumn("hour", hour(deals.messageTime)) \
        .withColumn("clientProvider", concat_ws('-', deals.client, deals.provider)) \
        .withColumn("storagePricePerEpochDouble", deals.storagePricePerEpoch.astype('double')) \
        .withColumn("providerCollateralDouble", deals.providerCollateral.astype('double')) \
        .withColumn("clientCollateralDouble", deals.clientCollateral.astype('double')) \
        .withColumn("pieceSizeDouble", deals.pieceSize.astype('double')) \
        .withColumn("lifetimeValue",
                    expr("storagePricePerEpochDouble * (endEpoch - startEpoch) * " +
                         "pieceSize / 1e18 / 1024 / 1024 / 1024")) 

    return deals
