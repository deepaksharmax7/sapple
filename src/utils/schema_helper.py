from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType




class SchemaMap:
    schema_map = {
        'items' : StructType([
                    StructField("id", IntegerType(), False),
                    StructField("name", StringType(), False),
                    StructField("description", StringType(), True),
                    StructField("price", DecimalType(10, 2), False),
                    StructField("quantity", IntegerType(), False)
                ])
    }


def fetch_schema(table):
    if table in SchemaMap.schema_map:
        return  SchemaMap.schema_map.get(table)
    else:
        print("No Schema Found")