# Databricks notebook source
# MAGIC %md
# MAGIC look up notebook will return the day number..lets say it returns 7 ..so we will pass this value to main notebook! to assign this value we need a parameter as below

# COMMAND ----------

dbutils.widgets.text("weekday","7")

# COMMAND ----------

# MAGIC %md
# MAGIC store this value

# COMMAND ----------

var = int(dbutils.widgets.get("weekday"))
print(type(var))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="weekoutput",value=var)