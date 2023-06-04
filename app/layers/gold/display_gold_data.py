import pyspark.sql.functions as F

from app.utils.DataUtils import DataUtils
from app.constants import PATH_GOLD_DATA


spark = DataUtils.initialize_spark()
df_breweries_delta = (
    spark
    .read.format("delta")
    .load(PATH_GOLD_DATA)
)

df_breweries_delta.show(10)