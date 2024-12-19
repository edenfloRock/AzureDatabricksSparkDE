# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Qualifying files

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Load files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp

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
    .json("dbfs:/mnt/edenflostoragedata/raw/qualifying/")



# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add columns

# COMMAND ----------

qualifying_final_df = qualifying_df \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp()) \

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container

# COMMAND ----------

qualifying_final_df.write \
    .mode("overwrite") \
    .parquet("/mnt/edenflostoragedata/processed/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/qualifying

# COMMAND ----------

df = spark.read.parquet("/mnt/edenflostoragedata/processed/qualifying")
df.count()

# COMMAND ----------

display(df)
