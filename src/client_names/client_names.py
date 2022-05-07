import os
from pyspark.sql.types import StructType

def get(spark, suffix=""):

    inputDir = 'input' + suffix
    inputDir = os.environ.get('INPUT_CLIENT_NAMES_DIR') or \
            base_dir + '/' + 'input' + suffix + '/client-names'

    schemaClientNames = StructType() \
        .add("clientName", "string") \
        .add("address", "string")

    names = spark \
        .read \
        .schema(schemaClientNames) \
        .json(inputDir + '/client-names.json')

    return names
