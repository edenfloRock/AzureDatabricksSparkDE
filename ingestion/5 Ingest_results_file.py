# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Stemp 1 - Load file with schema

# COMMAND ----------

from pyspark.sql.functions import concat, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType

# COMMAND ----------

#"":1,"":18,"":1,"":1,"":22,"":1,"":1,"":1,"":1,"":10,"":58,"":"1:34:50.616","":5690616,"":39,"":2,"":"1:27.452","":218.3,"":1
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
    .json("/mnt/edenflostoragedata/raw/results.json")
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
    .withColumn("ingestion_date", current_timestamp()) \
    .drop ("statusId")

display(results_renamed_df)

# COMMAND ----------

results_renamed_df.write.mode("overwrite").partitionBy("race_id") .parquet("/mnt/edenflostoragedata/processed/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/results

# COMMAND ----------

display(spark.read.parquet("/mnt/edenflostoragedata/processed/results"))

# COMMAND ----------


