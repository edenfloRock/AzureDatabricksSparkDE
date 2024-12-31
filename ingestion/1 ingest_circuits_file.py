# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# parameters
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "", "Data Source")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

# DBTITLE 1,run configuration Notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,run common_functions Notebook
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe  

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/raw

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv", header=True, inferSchema=True)

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

#circuits_df.show()
display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# DBTITLE 1,Se especifica esquema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType([ 
                              StructField('circuitId', IntegerType(), False),
                              StructField('circuitRef', StringType(), True),
                              StructField('name', StringType(), True),
                              StructField('location', StringType(), True),
                              StructField('country', StringType(), True),
                              StructField('lat', DoubleType(), True),
                              StructField('long', DoubleType(), True),
                              StructField('alt', IntegerType(), True),
                              StructField('url', StringType(), True)
])

circuits_df = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the requieres columns 

# COMMAND ----------

# DBTITLE 1,Form 1
circuits_selected_df = circuits_df.select("circuitId", "name", "location")
display(circuits_selected_df)

# COMMAND ----------

# DBTITLE 1,Form 2
circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.name, circuits_df.location)
display(circuits_selected_df)

# COMMAND ----------

# DBTITLE 1,Form 3
circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["name"], circuits_df["location"])
display(circuits_selected_df)

# COMMAND ----------

# DBTITLE 1,Form 4
# More flexible
from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId").alias("Circuit ID"), col("name"), col("location"))
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

# Columns no mentioned in the select() are no changed
circuits_renamed_df = circuits_df.withColumnRenamed("circuitId", "circuit_id") \
  .withColumnRenamed("circuitRef", "circuit_ref") \
  .withColumnRenamed("latitude", "latitudes") \
  .withColumnRenamed("long", "longitude") \
  .withColumnRenamed("alt", "altitudes") \
  .withColumn("data_source", lit(v_data_source))
display(circuits_renamed_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - ingestion_date (Get current timestamp)

# COMMAND ----------

# DBTITLE 1,call function: add_ingestion_date

circuits_final1_df = add_ingestion_date(circuits_renamed_df) 

display(circuits_final1_df)

# COMMAND ----------

# DBTITLE 1,Add new column: env
from pyspark.sql.functions import lit

circuits_final_df = circuits_final1_df.withColumn('env', lit('PRO'))

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Drop URL column

# COMMAND ----------

circuits_final_df = circuits_final_df.drop('url')
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 6 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/processed/circuits

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

# DBTITLE 1,Return "Success"
dbutils.notebook.exit("Success")
