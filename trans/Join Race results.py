# Databricks notebook source
# DBTITLE 1,Run configuration notebook
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# DBTITLE 1,Load races Dataframe
races_df = spark.read.parquet(f"{processed_folder_path}/races")
display(races_df)

# COMMAND ----------

# DBTITLE 1,Load Circuits Dataframe
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(circuits_df)

# COMMAND ----------

# DBTITLE 1,Join Races and Circuits
races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(
        races_df["race_id"],
        races_df["race_year"],
        races_df["name"].alias("race_name"),
        races_df["race_timestamp"].cast("date").alias("race_date"),        
        circuits_df["location"].alias("circuit_location")
    )
display(races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Load Results Dataframe
# driver_id, constructor_id, race_id
results_df = spark.read.parquet(f"{processed_folder_path}/results")
display(results_df)

# COMMAND ----------

# DBTITLE 1,Load Drivers Dataframe
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
display(drivers_df)

# COMMAND ----------

# DBTITLE 1,Join Results and Drivers
results_drivers_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .select(
        results_df["result_id"], 
        results_df["race_id"], 
        results_df["driver_id"], 
        results_df["constructor_id"], 
        
        drivers_df["name"].alias("driver_name"),
        drivers_df["number"].alias("driver_number"),
        drivers_df["nationality"].alias("driver_nationality"),
        
        results_df["grid"], 
        results_df["fastest_lap"], 
        results_df["time"].alias("race_time"),
        results_df["points"]
    )
display(results_drivers_df)

# COMMAND ----------

# DBTITLE 1,Load Constructors Dataframe
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
display(constructors_df)

# COMMAND ----------

# DBTITLE 1,Join Results, Drivers and Constructors
#constructor_id, 
results_drivers_constructors_df = results_drivers_df.join(constructors_df, results_drivers_df.constructor_id == constructors_df.constructor_id, "inner") \
    .select(
        results_drivers_df["*"], 
        constructors_df["name"].alias("team")
    )
display(results_drivers_constructors_df)

# COMMAND ----------

# DBTITLE 1,Join Results, Drivers, Constructors, Circuits andRaces
data_df = results_drivers_constructors_df.join(
    races_circuits_df, 
    results_drivers_constructors_df.race_id == races_circuits_df.race_id, 
    "inner") \
    .select(
        races_circuits_df["*"],
        results_drivers_constructors_df["*"]
    ) \
    .withColumn("created_date", current_timestamp()) \
    .select(
        "race_year",
        "race_name",
        "race_date",
        "circuit_location",
        "driver_name",
        "driver_number",
        "driver_nationality",
        "team",
        "grid",
        "fastest_lap",
        "race_time", 
        "points",        
        "created_date"
    )
display(data_df)

# COMMAND ----------

data_df

# COMMAND ----------

display(data_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'") \
    .orderBy(data_df.points.desc()))

# COMMAND ----------

data_df.write \
    .mode("overwrite") \
    .parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


