# Databricks notebook source
# DBTITLE 1,SETUP
import sys

sys.path.insert(0, "../lib")

import utils
import ingestors

# COMMAND ----------

# DBTITLE 1,Ingestão Full Load

catalog =  "silver"
schemaname =  "upsell"
tablename = dbutils.widgets.get("tablename")
idfield_old = dbutils.widgets.get("idfield_old")
id_field = dbutils.widgets.get("id_field")

remove_checkpoint = False

if not utils.table_exists(spark, catalog, schemaname, tablename):

    print(f"Criando a tabela [{tablename}] na camada Silver...")

    original_query = utils.import_query(f"{tablename}.sql")

    (spark.sql(original_query)
        .write
        .format("delta")
        .mode("overwrite")
        .option("OverWriteSchema", "true")
        .saveAsTable(f"silver.olist_ecommerce.{tablename}")
    )

    remove_checkpoint = True

else:
    print(f"Tabela [{tablename}] já existente na camada Silver.")

# COMMAND ----------

# DBTITLE 1,Ingestion CDF + Streaming
print("Iniciando CDF...")

ingest = ingestors.IngestionCDF(spark=spark, 
                                catalog=catalog, 
                                schemaname=schemaname, 
                                tablename=tablename, 
                                id_field=id_field, 
                                idfield_old=idfield_old)

if remove_checkpoint == True:
    dbutils.fs.rm(ingest.checkpoint_location, True)


stream = ingest.execute()
