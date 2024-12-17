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

# DBTITLE 1,3 Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
spark.conf.set("fs.azure.account.auth.type.edenflostorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.edenflostorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.edenflostorage.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.edenflostorage.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.edenflostorage.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# DBTITLE 1,4 Assign Role 'Storage Blob Data Contributor' to the Data Lake
#Go to storage account
#Click in Access Control AIM
# Click in Add 
# Add role assignment
# look for Storage Blob Data Contributor and select it
# Click Next
# Click Select members and look for your user for <name_of_service_principal> and select it
# Click Select
# Click Review + assign

# COMMAND ----------

files =dbutils.fs.ls("abfss://demo@edenflostorage.dfs.core.windows.net")
display(files)

# COMMAND ----------

files =dbutils.fs.ls("abfss://demo@edenflostorage.dfs.core.windows.net")
display(files)

# COMMAND ----------

display(spark.read.csv("abfss://demo@edenflostorage.dfs.core.windows.net/tablas_prueba_deletionvectors.csv"),header=True)

# COMMAND ----------


