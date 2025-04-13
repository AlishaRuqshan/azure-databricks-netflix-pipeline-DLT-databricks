# Databricks notebook source
# MAGIC %md
# MAGIC Silver Notebook Mapping ( Look up tables )

# COMMAND ----------

# MAGIC %md
# MAGIC Parameters

# COMMAND ----------

#Read the data 
df=spark.read.format("csv")\
.option("header", True)\
.option("inferschema", True)\
.load("abfss://bronze@sloc.dfs.core.windows.net/netflix_directors")

# COMMAND ----------

df.display()


# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
     .option("path","abfss://silver@sloc.dfs.core.windows.net/netflix_directors")\
         .save()


# COMMAND ----------

# MAGIC %md
# MAGIC PARAMETERS

# COMMAND ----------

dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder","netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC VARIABLES

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder")
var_trg_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

#Read the data 
df=spark.read.format("csv")\
.option("header", True)\
.option("inferschema", True)\
.load(f"abfss://bronze@sloc.dfs.core.windows.net/{var_src_folder}")


# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .option("path",f"abfss://silver@sloc.dfs.core.windows.net/{var_trg_folder}")\
        .save()