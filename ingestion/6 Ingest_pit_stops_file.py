# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "", "Data Source")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Red the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

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
    .json(f"{raw_folder_path}/pit_stops.json")

display(pits_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

pit_stops_final1_df = pits_stops_df \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumn("data_source", lit(v_data_source))

display(pit_stops_final1_df)

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_final1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to processed container in parquet format

# COMMAND ----------

pit_stops_final_df \
    .write.mode("overwrite") \
    .parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/pit_stops

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# DBTITLE 1,Return "Success"
dbutils.notebook.exit("Success")
