import delta
import utils

class Ingestion_Full_Load_In_Bronze:
        def __init__(self, spark, catalog, schemaname, tablename, data_format):
            self.spark = spark
            self.catalog = catalog
            self.schemaname = schemaname
            self.tablename = tablename
            self.data_format = data_format
            self.set_schema()


        def set_schema(self):
            self.df_schema_full = utils.import_schema_full(self.tablename)
        
        def load(self, path):
            df = (self.spark
                      .read
                      .format(self.data_format)
                      .option("header", "true")
                      .schema(self.df_schema_full)
                      .load(path))
            return df
        
        def save(self, df):
            (df.write
               .format("delta")    # Formato de salvamento do arquivo.
               .mode("overwrite")  # Modo de salvamento, aqui se caso ja contesse dados no Schema o "overwrite" iria subscreve-los.
               .saveAsTable(f"{self.catalog}.{self.schemaname}.{self.tablename}"))   # Aqui é onde dizemos para o Spark o caminho de salvamento, e especicamos que no Schema o arquivo será salvo como Tabela.
            return True
        
        def execute(self, path):
            df = self.load(path)
            return self.save(df)


class IngestionCDC(Ingestion_Full_Load_In_Bronze):
        def __init__(self, spark, catalog, schemaname, tablename, data_format, id_field, timestamp_field):
            super().__init__(spark, catalog, schemaname, tablename, data_format)
            self.id_field = id_field
            self.timestamp_field = timestamp_field
            self.set_schema_cdc()
            self.set_deltatable()

        def set_schema_cdc(self):
            self.df_schema_cdc = utils.import_schema_cdc(self.tablename)

        def set_deltatable(self):
            tablename =  f"{self.catalog}.{self.schemaname}.{self.tablename}"
            self.deltatable = delta.DeltaTable.forName(self.spark, tablename)
            
        def upsert(self, df):

            df.drop("modifed_at")
            df.createOrReplaceGlobalTempView(f"view_{self.tablename}")   # Global View pode ser acessada de qualquer sessão do Spark, pois é uma View Global.

            query = f''' 
                SELECT *
                FROM global_temp.view_{self.tablename}
                QUALIFY ROW_NUMBER() OVER(PARTITION BY {self.id_field} ORDER BY {self.timestamp_field} DESC) = 1
            '''     # Nesta parte do código estamos fazendo uma consulta SQL, onde tenho o objetivo de fazer um filtro 
                    # com que apareça os dados mais atualizados de cada cliente presente no arquivo CDC.

            df_cdc_table = self.spark.sql(query)    # Transformando o resultado da consulta SQL em um Dataframe.
            
        # Merge para atualizar a tabela com dados recentes dos arquivos CDC.
            (self.deltatable.alias("b")
                .merge(df_cdc_table.alias("c"), f"b.{self.id_field} = c.{self.id_field}")    # Fazendo um JOIN entre as tabelas.
                .whenMatchedDelete(condition = "c.OP = 'D'")      # Condição para deletar um dado presente na Tabela Bronze.
                .whenMatchedUpdateAll(condition = "c.OP = 'U'")   # Condição para atualizar um dado presente na Tabela Bronze
                .whenNotMatchedInsertAll(condition = "c.OP = 'I' OR c.OP = 'U'") # Condição para inserir um dado novo 
                                                                                    # na Tabela Bronze.
                .execute()

        ) 


        def load(self, path):
            df = (self.spark
                  .readStream  # Importando e fazendo a leitura de dados CDC com o Spark e Streaming.
                  .format("cloudFiles")
                  .option("cloudFiles.format", self.data_format)
                  .schema(self.df_schema_cdc)
                  .load(path))
            return df
        
        def save(self, df):
            stream = (df.writeStream
                        .option("checkpointLocation", f"/Volumes/raw/{self.schemaname}/cdc/{self.tablename}_checkpoint/")
                        # Pasta que controla o último progresso feito pelo stream.
                        .foreachBatch(lambda df, bathID: self.upsert(df)) 
                        # Para cada lote de dados que vier pelo stream, ele aplica uma função de upsert que recebe "df"(lote de dados), 
                        # faz a mesclagem com Merge e salva na base bronze (deltatable).
                        .trigger(availableNow=True))
            return stream.start()
