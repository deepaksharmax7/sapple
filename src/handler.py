import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import coalesce, col
from src.utils.secret_helper import get_source_conn, get_target_conn
from src.utils.schema_helper import fetch_schema


def load_table_wname(table_name, conn):

    # MySQL connection properties
    host = conn.get('host')
    port = conn.get('port')
    database = conn.get('database')
    user = conn.get('user')
    password = conn.get('password')     

    # MySQL JDBC URL
    jdbc_url = f"jdbc:mysql://{host}:{port}/{database}"


    # Read data from MySQL table into a DataFrame
    data = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()

    # Show the data
    data.show()

    return data

def write_table(table_name, data, conn):
    schema = fetch_schema(table_name)

    # Configure MySQL connection properties
    url = f"jdbc:mysql://{conn.get('host')}:{conn.get('port')}/{conn.get('database')}"
    properties = {
        "user": conn.get('user'),
        "password": conn.get('password')
    }

    data.write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)

def write_upsert(table_name, data, conn):
    schema = fetch_schema(table_name)
    # Configure MySQL connection properties
    url = f"jdbc:mysql://{conn.get('host')}:{conn.get('port')}/{conn.get('database')}"
    properties = {
        "user": conn.get('user'),
        "password": conn.get('password')
    }
    # Define the key columns that determine matching rows for upsert (replace with your actual key columns)
    key_columns = ["id"]

    data.write.format('jdbc')\
        .option("url", url)\
        .option("user", )
    data.write.jdbc(url=url, table=table_name, mode="append", properties=properties).insertInto(table_name) 


def main():
    
    table_name = 'items'
    src_df = load_table_wname(table_name, src_conn)
    target_df = load_table_wname(table_name, tgt_conn)
    # Define the key columns that determine matching rows for upsert (replace with your actual key columns)
    key_columns = ["id"]

    # Perform the upsert operation by merging the source_df with the target_df based on the key_columns
    # Updated rows will be updated in the target_df, and new rows will be inserted.
    merged_df = target_df \
        .join(src_df, key_columns, "outer") \
        .select(
            coalesce(src_df["id"], target_df["id"]).alias("id"),
            coalesce(src_df["name"], target_df["name"]).alias("name"),
            coalesce(src_df["description"], target_df["description"]).alias("description"),
            coalesce(src_df["price"], target_df["price"]).alias("price"),
            coalesce(src_df["quantity"], target_df["quantity"]).alias("quantity")
        )

    # Save the merged DataFrame back to the MySQL table
    write_table(table_name, merged_df, tgt_conn)
    print("Write complete")


    
if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("ReadAPP")\
        .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.1.0.jar")\
        .getOrCreate()
    
    src_conn = get_source_conn()
    tgt_conn = get_target_conn()
    main()

    # Stop the SparkSession
    spark.stop()
