# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[    
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[    
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/drivers.json", schema=drivers_schema)
display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 -  Renamed columns and add new columns
# MAGIC 1. driverid renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion_date added
# MAGIC 4. name added with concatenation of forname and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat,  lit

# COMMAND ----------

drivers_with_columns_df = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

display(drivers_with_columns_df)

# COMMAND ----------

drivers_ingestion_date_df = add_ingestion_date (drivers_with_columns_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3- Drop the unwanted columns
# MAGIC 1. name.forname
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_ingestion_date_df.drop("namer.forename", "namer.surname", "url")
display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/drivers

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))
