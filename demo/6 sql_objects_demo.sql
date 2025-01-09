-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Learn Objectives
-- MAGIC 1. Spark SQL documentation 
-- MAGIC 2. Create database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Spark documentation: 
-- MAGIC https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-database.html

-- COMMAND ----------

-- DBTITLE 1,2
create database demo

-- COMMAND ----------

create database if not exists demo

-- COMMAND ----------

-- DBTITLE 1,4
show databases;

-- COMMAND ----------

-- DBTITLE 1,5
describe database demo

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

-- DBTITLE 1,6
select current_database()

-- COMMAND ----------

use demo


-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------


