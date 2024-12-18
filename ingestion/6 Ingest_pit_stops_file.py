# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Red the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_schema = StructType(fields=[
  StructField("raceId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("stop", StringType(), True),
  StructField("lap", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("duration", StringType(), True),
  StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pits_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine", True) \
    .json("/mnt/edenflostoragedata/raw/pit_stops.json")

display(pits_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

pit_stops_final_df = pits_stops_df \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumn("ingestion_date", current_timestamp())

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

pit_stops_final_df \
    .write.mode("overwrite") \
    .parquet("/mnt/edenflostoragedata/processed/pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/pit_stops

# COMMAND ----------

display(spark.read.parquet("/mnt/edenflostoragedata/processed/pit_stops"))

# COMMAND ----------


