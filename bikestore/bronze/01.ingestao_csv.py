# Databricks notebook source
bronze_path   = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/bronze/'
silver_path   = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/silver/'
gold_path     = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/gold/'
resource_path = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/resource/origem/'
resource_path_volume = '/Volumes/bikestore/logistica/bikestore_resource/origem'

# COMMAND ----------

display(dbutils.fs.ls(resource_path))

# COMMAND ----------

files = dbutils.fs.ls(resource_path)
csv_files = [f.name for f in files if f.name.endswith('.csv')]

for file_name in csv_files:
    df_name = f"df_{file_name.replace('.csv', '')}"
    
    df = (spark.read
          .csv(f"{resource_path}/{file_name}", 
               header=True, 
               inferSchema=True, 
               sep=','))
    
    globals()[df_name] = df
    
    print(f"✅ DataFrame '{df_name}' criado com sucesso!")



# COMMAND ----------

print(csv_files)

# COMMAND ----------

# Lista os arquivos no diretório de origem
files = dbutils.fs.ls(resource_path)

# Filtra apenas arquivos CSV
csv_files = [f.name for f in files if f.name.endswith('.csv')]

# Loop para ler e salvar todos os CSVs
for file_name in csv_files:
    # Gera o nome do DataFrame e do destino
    df_name = f"df_{file_name.replace('.csv', '')}"
    table_name = file_name.replace('.csv', '')
    
    # Lê o CSV
    df = (spark.read
          .csv(f"{resource_path}/{file_name}",
               header=True,
               inferSchema=True,
               sep=',')
         )
    
    # Cria dinamicamente o DataFrame (opcional, se quiser acessá-lo depois)
    globals()[df_name] = df
    
    # Salva em formato Delta na camada Bronze
    (
        df.write
          .mode('overwrite')
          .format('delta')
          .option("mergeSchema", "true")
          .save(f"{bronze_path}/{table_name}")
    )
    
    print(f"✅ {df_name} salvo em Delta → {bronze_path}/{table_name}")
