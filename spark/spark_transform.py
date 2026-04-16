import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, year

# ── Configurações ──────────────────────────────────────
PROJECT_ID  = "propane-surfer-485218-m5"
BUCKET      = "propane-surfer-485218-m5-marsiqueiroz"
BQ_DATASET  = "energy_data"
BQ_TABLE    = "energy_staging"
KEY_PATH    = "/Users/marianasimoes/Desktop/Projetos/energy-pipeline/kestra/gcp-key.json"
JAR_PATH    = "/Users/marianasimoes/Desktop/Projetos/energy-pipeline/spark/jars/gcs-connector.jar"
BQ_JAR      = "/Users/marianasimoes/Desktop/Projetos/energy-pipeline/spark/jars/spark-bigquery-connector.jar"

# ── Iniciar o Spark ────────────────────────────────────
spark = SparkSession.builder \
    .appName("EnergyDataTransform") \
    .config("spark.jars", f"{JAR_PATH},{BQ_JAR}") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark iniciado com sucesso!")

# ── Ler o Parquet do GCS ───────────────────────────────
# Lê todos os arquivos dentro de raw/ (qualquer data)
INPUT_PATH = f"gs://{BUCKET}/raw/*/*.parquet"
print(f"Lendo dados de: {INPUT_PATH}")

df = spark.read.parquet(INPUT_PATH)
print(f"Total de linhas lidas: {df.count()}")

# ── Transformações ─────────────────────────────────────
# 1. Remover linhas sem país ou ano
df = df.filter(col("country").isNotNull() & col("year").isNotNull())

# 2. Selecionar colunas relevantes para o dashboard
colunas = [
    "country", "year", "iso_code", "population", "gdp",
    "primary_energy_consumption", "energy_per_capita", "energy_per_gdp",
    "fossil_fuel_consumption", "fossil_share_energy",
    "renewables_consumption", "renewables_share_energy",
    "coal_consumption", "coal_share_energy",
    "gas_consumption", "gas_share_energy",
    "oil_consumption", "oil_share_energy",
    "nuclear_consumption", "nuclear_share_energy",
    "solar_consumption", "solar_share_energy",
    "wind_consumption", "wind_share_energy",
    "hydro_consumption", "hydro_share_energy",
    "greenhouse_gas_emissions", "carbon_intensity_elec",
    "electricity_generation", "electricity_demand"
]
df = df.select(colunas)

# 3. Adicionar coluna de data de ingestão
df = df.withColumn("ingestion_date", current_date())

print(f"Linhas após limpeza: {df.count()}")
print("Amostra dos dados:")
df.show(5)

# ── Gravar no BigQuery ─────────────────────────────────
BQ_OUTPUT = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
print(f"Gravando em: {BQ_OUTPUT}")

df.write \
    .format("bigquery") \
    .option("table", BQ_OUTPUT) \
    .option("credentialsFile", KEY_PATH) \
    .option("parentProject", PROJECT_ID) \
    .option("temporaryGcsBucket", BUCKET) \
    .option("partitionField", "ingestion_date") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .mode("overwrite") \
    .save()

print("Dados gravados no BigQuery com sucesso!")
spark.stop()