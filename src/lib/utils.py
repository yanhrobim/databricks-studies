import json
from pyspark.sql import types

def table_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
            .filter(f"database = '{database}' AND tableName = '{table}'")
            .count())
    return count == 1
    

def import_schema_cdc(tablename):
    with open(f"{tablename}_schema_cdc.json", "r") as open_file:
            schema_json = json.load(open_file)  # Le o JSON e retorna em forma dicionário.
            
    df_schema = types.StructType.fromJson(schema_json)  # Definindo Schema Dataframe 
 # com StructType.
    return df_schema


def import_schema_full(tablename):
    with open(f"{tablename}_schema_full.json", "r") as open_file:
            schema_json = json.load(open_file)  # Le o JSON e retorna em forma dicionário.
            
    df_schema = types.StructType.fromJson(schema_json)  # Definindo Schema Dataframe
# com StructType.
    return df_schema


def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()


def extract_from(query: str):
    tablename = (query.lower()       # Transforma toda query em minúscula.
                      .split("from")[1]  # Pega todas strings a partir do from da query.
                      .strip()
                      .split(" ")[0]    # Remove todos os espaços em brancos da string. 
                      .split("\n")[0]   # Se contiver mais resultados depois do from, pega apenas o primeiro.
                                       # (Sendo a tabela na camada bronze.)
                      .strip())  # Remove todos os espaços em brancos da string novamente.
    return tablename

def add_new_from(query:str, new_from="df"):
    tablename = extract_from(query)
    query = query.replace(tablename, new_from).strip(" \n")
    return query


def add_columns(query:str, new_columns:list):
    select_query = query.split("FROM")[0].strip(" \n")  # Pega tudo antes do from. Ou seja nosso SELECT da query.
    
    add_new_colums = ",\n".join(new_columns)    # Adicionando as novas colunas através de uma lista.
                                                # também adiciona \n aos campos para as quebras de linhas dentro da query.
    
    from_query = f"\n\n FROM{query.split('FROM')[1]}"

    new_select_query = f"{select_query},\n{add_new_colums}{from_query}".strip()

    return new_select_query


def format_query_cdf(query:str, from_table):
    
    new_columns = ["_change_type", "_commit_version", "_commit_timestamp"]
    query = add_columns(query=query, new_columns=new_columns)
    query_cdf = add_new_from(query=query, new_from=from_table)

    return query_cdf