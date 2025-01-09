# Databricks notebook source
# MAGIC %md
# MAGIC # Produce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(
        sum("points").alias("total_points"), 
        count("race_year").alias("race_count"),
        count(when(col("position") == 1, True)).alias("wins")
        #sum(when(col("position") == 1, 1).otherwise(0)).alias("wins2")
    )
display(driver_standings_df)

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window  
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write \
    .mode("overwrite") \
    .parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/driver_standings")
display(df.filter("race_year = 2020"))
