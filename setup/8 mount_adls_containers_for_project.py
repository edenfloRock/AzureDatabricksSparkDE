# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ####Creating Mounts for adls using SAS token
# MAGIC

# COMMAND ----------

def create_mountpoint(storage_account_name, container_name, sas_token):
    
    sas_token = sas_token  
    container_name = container_name
    storage_account_name = storage_account_name
    mount_name = f"/mnt/{storage_account_name}/{container_name}"

    # validate if exits already
    if any(mount.mountPoint == mount_name for mount in dbutils.fs.mounts()):
        # unmount the mount point
        dbutils.fs.unmount(mount_name)

    dbutils.fs.mount(
        source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
        mount_point = mount_name,
        extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
    )
    
    display(dbutils.fs.mounts())


# COMMAND ----------

# DBTITLE 1,Mount Raw Container

SAS_string = dbutils.secrets.get(scope="edenflo-kv-scope", key="SAS-F1-Raw")
create_mountpoint ("edenflostoragedata", "raw", SAS_string)

# COMMAND ----------

# DBTITLE 1,Mount Presentation Container
SAS_string = dbutils.secrets.get(scope="edenflo-kv-scope", key="SAS-F1-Presentation")
create_mountpoint ("edenflostoragedata", "presentation", SAS_string)

# COMMAND ----------

# DBTITLE 1,Mount Processed Container

SAS_string = dbutils.secrets.get(scope="edenflo-kv-scope", key="SAS-F1-Processed")
create_mountpoint ("edenflostoragedata", "processed", SAS_string)

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#dbutils.fs.unmount("/mnt/edenflostoragedata/raw")

# COMMAND ----------


