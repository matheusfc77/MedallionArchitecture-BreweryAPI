import os
from pyspark.sql.types import StructField, StructType, StringType

SCHEMA_BRONZE_DATA = StructType([
    StructField('id',                              StringType(), True),
    StructField('name',                            StringType(), True),
    StructField('brewery_type',                    StringType(), True),
    StructField('address_1',                       StringType(), True),
    StructField('address_2',                       StringType(), True),
    StructField('address_3',                       StringType(), True),
    StructField('city',                            StringType(), True),
    StructField('state_province',                  StringType(), True),
    StructField('postal_code',                     StringType(), True),
    StructField('country',                         StringType(), True),
    StructField('longitude',                       StringType(), True),
    StructField('latitude',                        StringType(), True),
    StructField('phone',                           StringType(), True),
    StructField('website_url',                     StringType(), True),
    StructField('state',                           StringType(), True),
    StructField('street',                          StringType(), True),
])

PATH_ROOT = os.getcwd() + '/'
PATH_BRONZE_DATA = PATH_ROOT + "app/layers/bronze/delta_files"
PATH_SILVER_DATA = PATH_ROOT + "app/layers/silver/delta_files"
PATH_GOLD_DATA = PATH_ROOT + "app/layers/gold/delta_files"