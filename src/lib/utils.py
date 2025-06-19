import json
from pyspark.sql import types

def table_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
            .filter(f"database = '{database}' AND tableName = '{table}'")
            .count())
    return count == 1

def import_schema(tablename):
    with open(f"{tablename}_schema.json", "r") as open_file:
            schema_json = json.load(open_file)  # Le o JSON e retorna em forma dicionário.
            
    df_schema = types.StructType.fromJson(schema_json)  # Definindo Schema Dataframe com StructType.
    return df_schema