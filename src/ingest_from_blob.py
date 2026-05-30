# Databricks notebook source
# Your Azure Storage credentials
storage_account_name = "demostoragemasoud"
storage_account_key= dbutils.secrets.get(scope="azure_credentials", key="storage_account_key")
container_name = "democontainer"

print(f"Storage account: {storage_account_name}")
print(f"Container: {container_name}")

# COMMAND ----------

file_path = "sample.csv"

# Construct the blob path
blob_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

print(f"Reading from: {blob_path}")

# Read the CSV file with storage credentials passed as options
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key) \
    .load(blob_path)

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("id_lookup_table")
# COMMAND ----------

# COMMAND ----------