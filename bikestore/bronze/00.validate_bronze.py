# Databricks notebook source
from datetime import datetime
from pyspark.sql import Row

# Caminhos no Data Lake (ADLS Gen2)
bronze_path   = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/bronze'
silver_path   = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/silver'
gold_path     = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/gold'
resource_path = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/resource/origem'
controle_path = 'abfss://uc-ext-azure@externalazure28.dfs.core.windows.net/bikestore/bronze/log_bronze'

# Lista os arquivos CSV de origem
files = dbutils.fs.ls(resource_path)
csv_files = [f.name for f in files if f.name.endswith('.csv')]

# Lista para armazenar resultados da execução
resultados_execucao = []

# Loop principal
for file_name in csv_files:
    table_name = file_name.replace('.csv', '')
    df_name = f"df_{table_name}"
    status = "SUCESSO"
    data_execucao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Lê o CSV da camada resource
        df = (spark.read
              .csv(f"{resource_path}/{file_name}",
                   header=True,
                   inferSchema=True,
                   sep=',')
             )

        # Cria o DataFrame dinamicamente (opcional)
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

    except Exception as e:
        status = "ERRO"
        print(f"❌ Falha ao processar {file_name}: {e}")

    # Armazena o log da execução
    resultados_execucao.append(Row(
        tabela=table_name,
        status=status,
        data_execucao=data_execucao, 
        layer="bronze"
    ))

# Cria o DataFrame de controle
df_controle = spark.createDataFrame(resultados_execucao)

# Salva a tabela de controle (mantendo histórico)
(
    df_controle.write
      .mode("overwrite")
      .format("delta")
      .option("mergeSchema", "true")
      .save(controle_path)
)

print("📊 Tabela de controle atualizada com sucesso!")


# COMMAND ----------

display(df_controle)