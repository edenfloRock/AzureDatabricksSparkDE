# Databricks notebook source
# MAGIC %md
# MAGIC # Access dataframes using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from v_race_results 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from v_race_results where race_year = 2020

# COMMAND ----------

# DBTITLE 1,Read from pyheton cell
race_results0_df = spark.sql("select * from v_race_results")

# COMMAND ----------

race_results1_df = spark.sql("select * from v_race_results where race_year = 2020")

# COMMAND ----------

display(race_results1_df)

# COMMAND ----------

p_race_year= 2019
race_results2_df = spark.sql(f"select * from v_race_results where race_year = {p_race_year}")
display(race_results2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Global Temporary Views
# MAGIC 1. Create global temporary views on dataframe
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------


