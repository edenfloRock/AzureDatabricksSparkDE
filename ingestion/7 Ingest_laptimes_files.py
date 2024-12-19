# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest laptimes. folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schemas = StructType(    
    fields=[StructField("raceId", IntegerType(), False),
            StructField("driverId", IntegerType(), True),
            StructField("lap", IntegerType(), True),
            StructField("position", IntegerType(), True),
            StructField("time", StringType(), True),
            StructField("miliseconds", IntegerType(), True)
    ])

# COMMAND ----------

# DBTITLE 1,Form 1
# lap_times_df = spark.read \
#     .schema(lap_times_schemas) \
#     .csv("/mnt/edenflostoragedata/raw/lap_times/lap_time*.csv" )

# COMMAND ----------

# DBTITLE 1,Form 2
lap_times_df = spark.read \
    .schema(lap_times_schemas) \
    .csv(f"{raw_folder_path}/lap_times/" )

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# DBTITLE 1,Count
lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current_timestamp

# COMMAND ----------

lap_times_final1_df = lap_times_df \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id')
    
display(lap_times_final1_df)

# COMMAND ----------

# DBTITLE 1,Add ingestion_date column
lap_times_final_df = add_ingestion_date(lap_times_final1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

lap_times_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')


# COMMAND ----------

df = spark.read.parquet(f'{processed_folder_path}/lap_times')
display(df)

# COMMAND ----------

df.count()
