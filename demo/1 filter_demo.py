# Databricks notebook source
# MAGIC %md
# MAGIC # Filter Dmo

# COMMAND ----------

# DBTITLE 1,Run configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# DBTITLE 1,filter mode 1
races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")
display(races_filtered_df)


# COMMAND ----------

# DBTITLE 1,filter mode 2
races_filtered2_df = races_df.filter((races_df["race_year"] == 2018) & (races_df["round"] <= 5))
display(races_filtered2_df)


# COMMAND ----------

# DBTITLE 1,Filter mode 3
races_filtered3_df = races_df.filter((races_df.race_year == 2020) & (races_df.round <= 5))
display(races_filtered3_df)

# COMMAND ----------


