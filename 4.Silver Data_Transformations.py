# Databricks notebook source
# MAGIC %md
# MAGIC Silver Data Transformation

# COMMAND ----------

#Pull the library
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("abfss://bronze@sloc.dfs.core.windows.net/netflix_titles_foldee")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC when we loaded files from csv - in csv all columns are set as string cols ..so we will transform them 

# COMMAND ----------

# MAGIC %md
# MAGIC we will remove NULLS
# MAGIC

# COMMAND ----------

df = df.fillna({"duration_minutes" : 0, "duration_seasons":1})

# COMMAND ----------

df = df.withColumn("duration_minutes",col('duration_minutes').cast(IntegerType()))\
            .withColumn("duration_seasons",col('duration_seasons').cast(IntegerType()))

# COMMAND ----------


df.printSchema()

# COMMAND ----------

#fetch titles in to a new column - need to fetch titles values before : and dont need text after that
df = df.withColumn("Shorttitle",split(col('title'),':')[0])
df.display()

# COMMAND ----------

#we need to grab the first element of the ratinfg column, seperator is  - .
df = df.withColumn("rating",split(col('rating'),'-')[0])
df.display()

# COMMAND ----------

#conditional Column
#if my col is movie print 0 else 1 ( flag creation for future use for watever reason)
df = df.withColumn("type_flag",when(col('type') == 'Movie',1)
.when(col('type') == 'TV Show',2)
.otherwise(0))

df.display()

# COMMAND ----------

# If you want to rank the data without sort ! 
from pyspark.sql.window import Window
df = df.withColumn("duration_ranking",rank().over(Window.partitionBy("type").orderBy(col('duration_minutes').desc())))  

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC IF yoU ARE USING THIS TEMP VIEW IN THIS NOTEBOOK .. CAN WE USE IT IN OTHER NOTEBOOK..ANS IS NO!
# MAGIC
# MAGIC if they insist u need to use it out side of this notebook
# MAGIC then
# MAGIC you can use GLOBAL VIEW ! ( U can create GlobalTempView and use this) but it will be termitted if your session is closed
# MAGIC these are session based scope 

# COMMAND ----------

# MAGIC %md
# MAGIC v IMP : DATA AGGREGATION

# COMMAND ----------

# we will write the real data into silver
#rename  df to df_visualization
df_vis = df.groupBy("type").agg(count("*").alias("total_count"))
display(df_vis)

# COMMAND ----------

df.write.format("delta")\
.mode("overwrite")\
.option("path", "abfss://silver@sloc.dfs.core.windows.net/netflix_titles")\
.save()