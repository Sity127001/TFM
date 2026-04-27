from pyspark.sql import SparkSession

def get_spark():
    return (
        SparkSession.builder
        .appName("M5_pipeline")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )