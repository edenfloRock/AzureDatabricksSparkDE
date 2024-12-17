# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

# DBTITLE 1,1 Register Azure AD Application / Service Principal
# Go to Azure portal
# Look for Microsoft Entra ID
# Click App registrations
# Click + New registration
# Fill in the name field (<name_of_service_principal>) then click Register
# In Overview tab, copy the Application (client_id) ID and save it and paste it below
# Copy the Directory (tenant_id) ID and paste it below
# Click in pin button to pin to dashboard




# COMMAND ----------

# DBTITLE 1,2 Generate a secret/password for the Application
# In Certificates & secrets tab, click + New client secret
# Fill in the description field (<name_of_service_principal>-secret) then click Add
# Copy the value field and save it and paste it below

# COMMAND ----------

client_id = ""
tenant_id = ""
client_secret = ""

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(source="abfss://demo@edenflostorage.dfs.core.windows.net", 
                 mount_point="/mnt/formula1dl/demo", 
                 extra_configs=configs)

# COMMAND ----------

files =dbutils.fs.ls("/mnt/formula1dl/demo")
display(files)

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/tablas_prueba_deletionvectors.csv"),header=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#dbutils.fs.unmount("/mnt/formula1dl/demo")
