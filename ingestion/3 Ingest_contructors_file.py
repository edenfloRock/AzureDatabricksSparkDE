# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructor.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 -  Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/constructors.json")
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



contructor_final1_df = constructor_dropped_df \
  .withColumnRenamed("constructorId", "constructor_id") \
  .withColumnRenamed("constructorRef", "constructor_ref") \

display(contructor_final1_df)

# COMMAND ----------

# DBTITLE 1,Add ingestion_date column
contructor_final_df = add_ingestion_date(contructor_final1_df)
display(contructor_final_df)

# COMMAND ----------

contructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------


display(dbutils.fs.ls(f"{processed_folder_path}/constructors"))
