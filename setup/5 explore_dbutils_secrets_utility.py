# Databricks notebook source
# MAGIC %md
# MAGIC # Explore the capabilities of the debutils.secrets utilities

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('edenflo-kv-scope')

# COMMAND ----------

dbutils.secrets.get(scope='edenflo-kv-scope', key='SAS-F1-Data')

# COMMAND ----------


