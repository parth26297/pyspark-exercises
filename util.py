from pyspark.sql import SparkSession
from delta import *

def get_spark_session():
    builder = SparkSession.builder.appName("SparkExercisesApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .enableHiveSupport()

    return configure_spark_with_delta_pip(builder, ["com.databricks:spark-xml_2.12:0.16.0"]).getOrCreate()
