# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey="WeekdayLookup", key="weekoutput", debugValue="default_value")

# COMMAND ----------

print(var)