-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create External Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learn Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- DBTITLE 1,Load data
-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- DBTITLE 1,Save as a table
-- MAGIC %python
-- MAGIC race_results_df.write\
-- MAGIC     .mode("overwrite")\
-- MAGIC     .format("parquet")\
-- MAGIC     .option("path", f"{presentation_folder_path}/race_results_ext_py")\
-- MAGIC     .saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe table extended demo.race_results_ext_py

-- COMMAND ----------

select * from demo.race_results_ext_py where race_year=2020

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/edenflostoragedata/presentation/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.printSchema()

-- COMMAND ----------

create table demo.race_results_ext_sql (
race_year INT,
race_name STRING,
race_date DATE,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points INT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION '/mnt/edenflostoragedata/presentation/race_results_ext_sql'


-- COMMAND ----------

describe table extended demo.race_results_ext_sql

-- COMMAND ----------

insert into demo.race_results_ext_sql 
 select * from demo.race_results_ext_py where race_year=2020

-- COMMAND ----------

select * from demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/edenflostoragedata/presentation/")

-- COMMAND ----------

show tables in demo

-- COMMAND ----------


drop table demo.race_results_ext_sql

-- COMMAND ----------

-- the table doesn't exist in the metadata
show tables in demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # but drop table comand doesn't remove the folder, because the table is external
-- MAGIC dbutils.fs.ls("/mnt/edenflostoragedata/presentation/")

-- COMMAND ----------

-- drop the race_results_ext_py table 
drop table demo.race_results_ext_py;
show tables in demo;

-- COMMAND ----------

-- DBTITLE 1,Drop files because is a presentation path
-- MAGIC %python
-- MAGIC dbutils.fs.rm ("/mnt/edenflostoragedata/presentation/race_results_ext_py", True)
-- MAGIC dbutils.fs.rm ("/mnt/edenflostoragedata/presentation/race_results_ext_sql", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/edenflostoragedata/presentation/")

-- COMMAND ----------


