# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "", "Data Source")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Stemp 1 - Load file with schema

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType

# COMMAND ----------

results_schema = StructType([ 
    StructField("resultId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", DecimalType(), True),
    StructField("statusId", IntegerType(), True),

])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/results.json")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and drop statusId column

# COMMAND ----------

results_renamed_df = results_df \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source)) \
    .drop ("statusId")

display(results_renamed_df)

# COMMAND ----------

results_final_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

results_final_df.write \
    .mode("overwrite") \
    .partitionBy("race_id") \
    .parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/results

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# DBTITLE 1,Return "Success"
dbutils.notebook.exit("Success")
