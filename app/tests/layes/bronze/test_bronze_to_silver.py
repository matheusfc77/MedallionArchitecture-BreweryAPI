import pyspark.sql.functions as F

from app.utils.DataUtils import DataUtils
from app.constants import PATH_SILVER_DATA


spark = DataUtils.initialize_spark()
df_breweries_delta = (
    spark
    .read.format("delta")
    .load(PATH_SILVER_DATA)
)

def test_duplicated_id():
    expected = []
    actual = DataUtils.duplicated_rows_by_id(df_breweries_delta, 'id')
    message = 'Duplicated ids: ' + str(actual)
    assert actual == expected, message


def test_null_id():
    expected = 0
    actual = df_breweries_delta.filter(F.col('id').isNull()).count()
    message = '{} records with column [id] NULL'.format(actual)
    assert actual == expected, message


def test_brewery_type():
    values = df_breweries_delta.select('brewery_type').distinct().collect()
    values = [row['brewery_type'] for row in values]

    valids = ['micro', 'nano', 'regional', 'brewpub', 'large', 'planning', 'bar', 'contract', 'proprietor', 'closed']

    expected = []
    actual = [vl for vl in values if vl not in valids]
    message = 'Invalid brewery types: {}'.format(actual)
    assert actual == expected, message


def test_postal_code_format():
    expected = []
    actual = (
        df_breweries_delta
            .withColumn('postal_code_format', F.regexp_replace('postal_code', r"\d", "D"))
            .filter(~(
                (F.col('postal_code_format') == 'DDDDD-DDDD') |
                (F.col('postal_code_format') == 'DDDDD')
            ))
            .select('id')
            .collect()
    )
    actual = [row['id'] for row in actual]
    message = '{} records with invalid [postal_code] format. Ids: {}'.format(len(actual), actual)
    assert actual == expected, message


def test_country():
    values = (
        df_breweries_delta
            .withColumn('country_fmt', F.lower(F.trim(F.col('country'))))
            .select('country_fmt')
            .distinct()
            .collect()
    )
    values = [row['country_fmt'] for row in values]
    valids = ['portugal', 'poland', 'south korea', 'austria', 'isle of man', 'france', 'united states', 'scotland', 'ireland', 'england']

    expected = []
    actual = [vl for vl in values if vl not in valids]
    message = 'Invalid countries: {}'.format(actual)
    assert actual == expected, message


def test_longitude():
    expected = []
    actual = (
        df_breweries_delta
            .filter(F.col('longitude').like('%,%'))
            .select('id')
            .collect()
    )
    actual = [row['id'] for row in actual]
    message = '{} records with , (comma) instead . (dot) as separator. Ids: {}'.format(len(actual), actual)
    assert actual == expected, message

    actual = (
        df_breweries_delta
            .filter(
                (F.col('longitude') < -180) |
                (F.col('longitude') > 180)
            )
            .select('id')
            .collect()
    )
    actual = [row['id'] for row in actual]
    message = '{} records with invalid longitude (>180 or <-180). Ids: {}'.format(len(actual), actual)
    assert actual == expected, message


def test_latitude():
    expected = []
    actual = (
        df_breweries_delta
            .filter(F.col('latitude').like('%,%'))
            .select('id')
            .collect()
    )
    actual = [row['id'] for row in actual]
    message = '{} records with , (comma) instead . (dot) as separator. Ids: {}'.format(len(actual), actual)
    assert actual == expected, message

    actual = (
        df_breweries_delta
            .filter(
                (F.col('latitude') < -90) |
                (F.col('latitude') > 90)
            )
            .select('id')
            .collect()
    )
    actual = [row['id'] for row in actual]
    message = '{} records with invalid latitude (>90 or <-90). Ids: {}'.format(len(actual), actual)
    assert actual == expected, message   


def test_website_url():
    expected = []
    actual = (
        df_breweries_delta
        .filter(
            (~F.col('website_url').like('http%')) &
            (F.col('website_url') != '')
        )
        .select('id')
        .collect()
    )
    actual = [row['id'] for row in actual]
    message = '{} records with invalid [website_url]. Ids: {}'.format(len(actual), actual)
    assert actual == expected, message


test_duplicated_id()
test_null_id()

# taproom does not listed in site but exists in real life, so I preferred to keep
#test_brewery_type()

# 97% of the records have EUA format for postal code but, as exist other countries, I did not filter this field
# test_postal_code_format()

test_country()
test_longitude()
test_latitude()
test_website_url()