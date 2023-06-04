import pyspark.sql.functions as F

from app.utils.DataUtils import DataUtils
from app.constants import PATH_BRONZE_DATA, PATH_SILVER_DATA
from app.utils.now import Now

print(Now().now(), 'START BRONZE TO SILVER')

# read raw data, apply treatments and persist in delta format

spark = DataUtils.initialize_spark()
df_breweries_delta = (
    spark
    .read.format("delta")
    .load(PATH_BRONZE_DATA)
)

record_duplicated = DataUtils.duplicated_rows_by_id(df_breweries_delta, 'id')
assert record_duplicated == [], 'Field [id] duplicated. {}'.format(record_duplicated)

df_breweries_delta = (
    df_breweries_delta
        .withColumn('name', F.trim(F.lower(F.col('name'))))
        .withColumn('brewery_type', F.lower(F.col('brewery_type')))

        # about 99% of the records don't have information in [address_2] or [address_3], so I concatenated of the fields
        .withColumn('address', F.concat(
            F.when(F.col('address_1').isNull(), '').otherwise(F.col('address_1')), F.lit('|'),
            F.when(F.col('address_2').isNull(), '').otherwise(F.col('address_2')), F.lit('|'),
            F.when(F.col('address_3').isNull(), '').otherwise(F.col('address_3')),
        ))
        .withColumn('address', F.when(F.col('address') == '||', F.lit(None)).otherwise(F.col('address')))
        .withColumn('address', F.trim(F.lower(F.regexp_replace('address', '\|\|', ''))))
        .drop('address_1', 'address_2', 'address_3')

        .withColumn('city', F.lower(F.col('city')))

        # all records have same information in [state]
        .drop('state_province')

        .withColumn('country', F.trim(F.lower(F.col('country'))))

        .withColumn('latitude', F.regexp_replace('latitude', ',', '.').cast('float'))
        .withColumn('latitude', F.when(
            (F.col('latitude') < -90) |
            (F.col('latitude') > 90), 0).otherwise(F.col('latitude'))
        )

        .withColumn('longitude', F.regexp_replace('longitude', ',', '.').cast('float'))
        .withColumn('longitude', F.when(
            (F.col('longitude') < -180) |
            (F.col('longitude') > 180), 0).otherwise(F.col('longitude'))
        )

        .withColumn('website_url', F.when(F.col('website_url').like('http%'), F.col('website_url')).otherwise(''))

        .withColumn('state', F.trim(F.lower(F.col('state'))))
        .withColumn('street', F.trim(F.lower(F.col('street'))))
)

df_breweries_delta\
    .write.format("delta")\
    .mode("overwrite")\
    .partitionBy('city')\
    .save(PATH_SILVER_DATA)

print(Now().now(), 'END BRONZE TO SILVER')