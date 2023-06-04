from app.utils.FileUtils import FileUtils
from app.constants import PATH_ROOT


FileUtils.create_folder(PATH_ROOT + 'app/data')

# verify brewery API is OK
import app.tests.extractor.test_brewery_api

# extract brewery data and save JSON format
import app.extractor.extract_data_api

# load JSONs and save as delta format
import app.layers.bronze.source_to_delta

# load raw data, apply treatments and save in delta format partitioned by city
import app.layers.bronze.bronze_to_silver

# verify data in silver layer
import app.tests.layes.bronze.test_bronze_to_silver

# load data in silver layer, group by city and brewery type and persist in gold layer
import app.layers.silver.silver_to_gold

# verify data in gold layer
import app.tests.layes.silver.test_silver_to_gold

# show data in gold layer
import app.layers.gold.display_gold_data