# Databricks notebook source
#dbutils.notebook.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "", "Data Source")

# COMMAND ----------

v_source_Ergast_API = dbutils.widgets.get("p_data_source")
print(v_source_Ergast_API)

# COMMAND ----------

#v_source_Ergast_API = "Ergast API"

# COMMAND ----------

# DBTITLE 1,Run 1 ingest circuits file
v_status = dbutils.notebook.run("1 ingest_circuits_file", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")

# COMMAND ----------

# DBTITLE 1,Run 2 ingest races file
v_status = dbutils.notebook.run("2 Ingest_races_file", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")

# COMMAND ----------

# DBTITLE 1,Run 3 Ingest constructors file
v_status = dbutils.notebook.run("3 Ingest_contructors_file", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")

# COMMAND ----------

# DBTITLE 1,Run 4 Ingest drivers files
v_status = dbutils.notebook.run("4 Ingest_drivers_file", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")

# COMMAND ----------

# DBTITLE 1,Run 5 Ingest results file
v_status = dbutils.notebook.run("5 Ingest_results_file", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")

# COMMAND ----------

# DBTITLE 1,Run 6 Ingest pit stops file
v_status = dbutils.notebook.run("6 Ingest_pit_stops_file", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")

# COMMAND ----------

# DBTITLE 1,Run 7 Ingest laptimes file
v_status = dbutils.notebook.run("7 Ingest_laptimes_files", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")

# COMMAND ----------

# DBTITLE 1,Run 8 Ingest qualifying
v_status = dbutils.notebook.run("8 Ingest_qualifying_files", 0, {"p_data_source":v_source_Ergast_API})

# COMMAND ----------

print (f"v_status: {v_status}")
