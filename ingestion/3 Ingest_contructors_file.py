# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructor.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 -  Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json("/mnt/edenflostoragedata/raw/constructors.json")
display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

contructor_final_df = constructor_dropped_df \
  .withColumnRenamed("constructorId", "constructor_id") \
  .withColumnRenamed("constructorRef", "constructor_ref") \
  .withColumn("ingestion_date", current_timestamp()
)
display(contructor_final_df)

# COMMAND ----------

contructor_final_df.write.mode("overwrite").parquet("/mnt/edenflostoragedata/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/constructors
