# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv files

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 Load races.csv file

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

races_schema = StructType([ 
                              StructField('raceId', IntegerType(), False),
                              StructField('year', IntegerType(), True),
                              StructField('round', IntegerType(), True),
                              StructField('circuitId', IntegerType(), True),                              
                              StructField('name', StringType(), True),
                              StructField('date', DateType(), True),
                              StructField('time', StringType(), True),
                              StructField('url', StringType(), True)
])

races_df = spark.read \
    .option('header', True) \
    .schema(races_schema) \
    .csv('dbfs:/mnt/edenflostoragedata/raw/races.csv')

display(races_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select required columns

# COMMAND ----------

 from pyspark.sql.functions import col
races_selected_df = races_df.select(col("raceId"), col("year"), col("round"),  col("circuitId"), col("name"), col("date"), col("time"))

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns

# COMMAND ----------

races_renamed_df = races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"),  col("circuitId").alias("circuit_id"), col("name"), col("date"), col("time"))

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Calculate race_timestamp column and drop date and time columns

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, current_timestamp

races_final_df = races_renamed_df \
  .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss" )) \
  .withColumn("ingestion_date", current_timestamp()) \
  .drop("date") \
  .drop("time")
display(races_final_df)

# COMMAND ----------

races_final_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write parquet file

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/edenflostoragedata/processed/races")

# COMMAND ----------

display( spark.read.parquet("/mnt/edenflostoragedata/processed/races"))

