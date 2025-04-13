# Databricks notebook source
# MAGIC %md
# MAGIC Loading incremental data 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA catalog_netflix.net_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CATALOG catalog_netflix;

# COMMAND ----------

checkpoint_location = "abfss://silver@sloc.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_location)\
  .load("abfss://rawsloc@sloc.dfs.core.windows.net")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC data is stored in delta format cz we didnt define any format .. so it bydefaults write it in delta format
# MAGIC

# COMMAND ----------

df.writeStream\
  .option("checkpointLocation", checkpoint_location)\
  .trigger(processingTime='10 seconds')\
  .start("abfss://bronze@sloc.dfs.core.windows.net/netflix_titles_foldee")

# COMMAND ----------

# MAGIC %md
# MAGIC Df.display , df.show, df.collect , df.write is a action and needs to be cancelled if u dont need bill

# COMMAND ----------

# MAGIC %md
# MAGIC