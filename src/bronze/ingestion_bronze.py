# Databricks notebook source
import delta
import sys
import json

sys.path.insert(0, "../lib/")   # Importando funções/classes da Lib.

import json
import utils
import ingestors

# COMMAND ----------

catalog = 'bronze'
schemaname = 'upsell'

tablename = dbutils.widgets.get('tablename')
id_field = dbutils.widgets.get('id_field')
timestamp_field = dbutils.widgets.get('timestamp_field')

checkpoint_location = f"/Volumes/raw/{schemaname}/cdc/{tablename}_checkpoint/"
full_load_path = f"/Volumes/raw/{schemaname}/full_load/{tablename}/"
cdc_path = f"/Volumes/raw/{schemaname}/cdc/{tablename}/"

# COMMAND ----------

# Esta camada de código tem o objetivo de fazer a ingestão de um arquivo Full Load na pasta de Bronze. 
# O arquivo Full Load é uma Tabela em formato CSV, que contém dados mas que precisam ser atualizados através de arquivos
# CDC (Change Data Capture).

# Nesta parte do código, precisamos primeiro ler o arquivo CSV com o Spark que logo após a leitura se transforma em um
# Dataframe. 
# No código podemos notar .option("header", "true"), utilizei esse comando pois com ele conseguimos nos comunicar
# com o Spark e dizer que o arquivo CSV contém cabeçalho, ou seja, nome de colunas presentes na primeira.

if not utils.table_exists(spark, catalog, schemaname, tablename): # Utilizando um IF, Se a tabela existir a função retorna 1 
# e não faz nada. Mas se a tabela não existir faz a ingestão da full load na camada de Bronze.

  print('Tabela não existente. Criando...')

  dbutils.fs.rm(checkpoint_location, True)

  ingest_full_load = ingestors.Ingestion_Full_Load_In_Bronze(spark=spark,
                                                            catalog=catalog, 
                                                            schemaname=schemaname, 
                                                            tablename=tablename, 
                                                            data_format="csv")
  
  ingest_full_load.execute(full_load_path)

  print("Tabela criada com sucesso !!!")
  
else:
  print('Tabela já existente :/')

# COMMAND ----------

# Nesta camada de código preciso da importação de arquivos CDC. Onde no futuro esses arquivos que precisam ser atualizados
# serão a principal fonte de dados para a atualização da Tabela presente no Schema Bronze.

ingest_cdc = ingestors.IngestionCDC(spark=spark,
                                    catalog=catalog,
                                    schemaname=schemaname,
                                    tablename=tablename,
                                    data_format="csv",
                                    id_field=id_field,
                                    timestamp_field=timestamp_field)

stream = ingest_cdc.execute(cdc_path)
