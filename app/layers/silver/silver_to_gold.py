import pyspark.sql.functions as F

from app.utils.DataUtils import DataUtils
from app.constants import PATH_SILVER_DATA, PATH_GOLD_DATA
from app.utils.now import Now


print(Now().now(), 'START SILVER TO GOLD')

# read silver data, group by city, brewery_type and save in gold layer

spark = DataUtils.initialize_spark()
df_breweries_delta = (
    spark
    .read.format("delta")
    .load(PATH_SILVER_DATA)
)

record_duplicated = DataUtils.duplicated_rows_by_id(df_breweries_delta, 'id')
assert record_duplicated == [], 'Field [id] duplicated. {}'.format(record_duplicated)

df_breweries_delta = (
    df_breweries_delta
        .groupBy('city', 'brewery_type')
        .count()
)

df_breweries_delta\
    .write.format("delta")\
    .mode("overwrite")\
    .save(PATH_GOLD_DATA)

print(Now().now(), 'END SILVER TO GOLD')