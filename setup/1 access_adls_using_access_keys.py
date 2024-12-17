# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from csv file

# COMMAND ----------

# Go to Azure portal
# Look for ADLS
# Clic Access keys in Security + networking
# Copy key1



# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope = "edenflo-kv-scope", key = "AccessKey-F1-Data")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.edenflostoragedata.dfs.core.windows.net", 
    formula1_account_key
)

# COMMAND ----------

files =dbutils.fs.ls("abfss://demo@edenflostoragedata.dfs.core.windows.net")
display(files)

# COMMAND ----------

print(files)

# COMMAND ----------

for file in files:
    print(file[0])

# COMMAND ----------

display(spark.read.csv("abfss://demo@edenflostoragedata.dfs.core.windows.net/circuits.csv"),header=True)

# COMMAND ----------


