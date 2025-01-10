-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Managed Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learn Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
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
-- MAGIC     .saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe table extended demo.race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year=2020

-- COMMAND ----------

create table demo.race_results_sql as select * from demo.race_results_python where race_year=2020

-- COMMAND ----------

describe table extended demo.race_results_sql

-- COMMAND ----------

select * from demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls ("dbfs:/user/hive/warehouse/demo.db/")
-- MAGIC

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # drop table command drop the files in the directory because the table is (or was) managed
-- MAGIC dbutils.fs.ls ("dbfs:/user/hive/warehouse/demo.db/")
-- MAGIC

-- COMMAND ----------


