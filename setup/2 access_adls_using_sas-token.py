# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from csv file

# COMMAND ----------

formula1_account_SAS = dbutils.secrets.get(scope="edenflo-kv-scope", key="SAS-F1-Data")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.edenflostoragedata.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.edenflostoragedata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.edenflostoragedata.dfs.core.windows.net", formula1_account_SAS)

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


