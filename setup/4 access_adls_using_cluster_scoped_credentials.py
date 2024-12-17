# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster_scoped_credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from csv file

# COMMAND ----------

# on cluster's spark config
#fs.azure.account.key.edenflostoragedata.dfs.core.windows.net {{secrets/edenflo-kv-scope/AccessKey-F1-Data}}

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


