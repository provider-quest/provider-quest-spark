from pyspark.sql.types import StructType

def process_client_names(spark, suffix=""):

    inputDir = 'input' + suffix

    schemaClientNames = StructType() \
        .add("clientName", "string") \
        .add("address", "string")

    names = spark \
        .read \
        .schema(schemaClientNames) \
        .json(inputDir + '/client-names/client-names.json')

    return names
