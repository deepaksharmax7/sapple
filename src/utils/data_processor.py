# data_processor.py

from pyspark.sql import SparkSession

def read_data_from_table(table_name, jdbc_url):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format("jdbc").options(url=jdbc_url, dbtable=table_name).load()
    return df
