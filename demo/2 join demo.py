# Databricks notebook source
# MAGIC %md
# MAGIC # Join Demo

# COMMAND ----------

# DBTITLE 1,Run configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")
display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(circuits_df)

# COMMAND ----------

# DBTITLE 1,Join
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(
        circuits_df["name"].alias("circuit_name"),
        circuits_df["location"],
        circuits_df["country"],
        races_df["name"].alias("race_name"),
        races_df["round"]
    )
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Left
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "left") \
    .select(
        circuits_df["name"].alias("circuit_name"),
        circuits_df["location"],
        circuits_df["country"],
        races_df["name"].alias("race_name"),
        races_df["round"]
    )
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Right
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "right") \
    .select(
        circuits_df["name"].alias("circuit_name"),
        circuits_df["location"],
        circuits_df["country"],
        races_df["name"].alias("race_name"),
        races_df["round"]
    )
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Full
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "full") \
    .select(
        circuits_df["name"].alias("circuit_name"),
        circuits_df["location"],
        circuits_df["country"],
        races_df["name"].alias("race_name"),
        races_df["round"]
    )
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Semi 1
# Similar to Join but only returns columns from the left Dataframe
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "semi") 
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Semi 2
# Similar to Join but only returns columns from the left Dataframe
races_circuits_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id, "semi") 
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Anti 1
races_circuits_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id, "anti") 
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Anti 2
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "anti") 
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Cross
races_circuits_df = races_df.crossJoin(circuits_df) 
display(races_circuits_df)

# COMMAND ----------


