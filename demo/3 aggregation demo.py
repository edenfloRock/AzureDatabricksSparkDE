# Databricks notebook source
# DBTITLE 1,Run configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregate Functions Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bult-in Aggregate Functions

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

demo_df = races_results_df.filter("race_year = 2020")
display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, avg, max, min, sum, col, lit

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'") \
    .select(
        sum("points").alias("total_points"),
        countDistinct("race_name").alias("total_of_races"),
    ).show()

# COMMAND ----------

demo_df\
    .groupBy("driver_name")\
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_of_races"))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Windows functions

# COMMAND ----------

demo_df = races_results_df.filter("race_year in (2019, 2020)")
display(demo_df)

# COMMAND ----------

demo_groups_df = demo_df\
    .groupBy("race_year", "driver_name")\
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_of_races"))\
    

# COMMAND ----------

display(demo_groups_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_groups_df.withColumn("rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------


