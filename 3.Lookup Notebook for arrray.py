# Databricks notebook source
# MAGIC %md
# MAGIC Array PARAMTER

# COMMAND ----------


files = [
    {
        "sourcefolder" : "netflix_cast",
        "targetfolder"  : "netflix_cast"
    },
    {
        "sourcefolder" : "netflix_category",
        "targetfolder"  : "netflix_category"
    },
    {
        "sourcefolder" : "netflix_countries",
        "targetfolder"  : "netflix_countries"
    },
    {
        "sourcefolder" : "netflix_directors",
        "targetfolder"  : "netflix_directors"
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC JOB UTILITY TO RETURN ARRAY

# COMMAND ----------

dbutils.jobs.taskValues.set("my_Array", files)