import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip


class DataUtils:
    '''
    Class with useful methods for data manipulation
    '''

    def initialize_spark():
        spark = (
            SparkSession
            .builder.master("local")
            .appName("myapp")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        spark = configure_spark_with_delta_pip(spark).getOrCreate()
        #spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.shuffle.partitions", 8)
        return spark
    

    def duplicated_rows_by_id(data, id_column='id'):
        return data.select(id_column).groupBy(id_column).count().filter(F.col('count') > 1).collect()
    
