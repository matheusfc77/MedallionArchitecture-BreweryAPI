import pyspark.sql.functions as F

from app.utils.DataUtils import DataUtils
from app.constants import PATH_GOLD_DATA


spark = DataUtils.initialize_spark()
df_breweries_delta = (
    spark
    .read.format("delta")
    .load(PATH_GOLD_DATA)
)

def test_outliers():

    # # usually I use IQR Method to identify outliers but this doesn't work here (many rows with 1 or 2 breweries)
    # p25, p75 = (
    #     df_breweries_delta
    #         .select(F.percentile_approx(F.col('count'), [.25, .75]).alias('quantiles'))
    #         .collect()
    # )
    # iqr = p75 - p25
    # upper_bound = iqr * 1.5
    
    upper_bound = 70
    expected = []
    actual = (
        df_breweries_delta
            .filter(F.col('count') > upper_bound)
            .select('city', 'brewery_type', 'count')
            .collect()
    )
    message = 'Outliers detected (over {}): {}'.format(upper_bound, actual)
    assert actual == expected, message


test_outliers()