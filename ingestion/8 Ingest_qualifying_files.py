# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Qualifying files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "", "Data Source")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Load files

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# DBTITLE 1,Define schema
schema_qualifying = StructType([ 
    StructField("qualifyingId", IntegerType(), True), \
    StructField("raceId", IntegerType(), True), \
    StructField("driverId", IntegerType(), True), \
    StructField("constructorId", IntegerType(), True), \
    StructField("number", IntegerType(), True), \
    StructField("position", IntegerType(), True), \
    StructField("q1", StringType(), True), \
    StructField("q2", StringType(), True), \
    StructField("q3", StringType(), True) 
    ])


# COMMAND ----------

qualifying_df = spark.read \
    .option("schema", schema_qualifying) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/qualifying/")



# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add columns

# COMMAND ----------

qualifying_final1_df = qualifying_df \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("data_source", lit(v_data_source))

display(qualifying_final1_df)

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container

# COMMAND ----------

qualifying_final_df.write \
    .mode("overwrite") \
    .parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/qualifying

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/qualifying")
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Return "Success"
dbutils.notebook.exit("Success")
