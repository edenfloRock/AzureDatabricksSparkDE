# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe  

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/edenflostoragedata/raw

# COMMAND ----------

circuits_df = spark.read.csv('dbfs:/mnt/edenflostoragedata/raw/circuits.csv', header=True, inferSchema=True)

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
    .csv('dbfs:/mnt/edenflostoragedata/raw/circuits.csv')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------


