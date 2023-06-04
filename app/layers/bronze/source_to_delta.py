from glob import glob

from app.utils.DataUtils import DataUtils
from app.constants import SCHEMA_BRONZE_DATA, PATH_BRONZE_DATA


# read last API extraction and save in delta format

spark = DataUtils.initialize_spark()
folders = glob("app/data/*/", recursive = True)

last_extraction = max([int(fd.split('/')[2].replace('T', '')) for fd in folders])
last_extraction_formated = str(last_extraction)[:8] + 'T' + str(last_extraction)[8:]

df_breweries = (
    spark
    .read.format('json')
    .schema(SCHEMA_BRONZE_DATA)
    .load('app/data/' + last_extraction_formated)
)

df_breweries\
    .write.format("delta")\
    .mode("overwrite")\
    .save(PATH_BRONZE_DATA)
