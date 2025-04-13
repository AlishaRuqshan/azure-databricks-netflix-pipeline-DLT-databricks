# Databricks notebook source
# MAGIC %md
# MAGIC DLT ( Delta Live Tables) is a declarative ETL framework
# MAGIC three Pillars:
# MAGIC 1. Streaming Table
# MAGIC 2. Materialized View
# MAGIC 3. Views : - a. Normal view ; b.Streaming Views

# COMMAND ----------

# MAGIC %md
# MAGIC Expectations: Data Quality that is also known as expectation in DLT
# MAGIC has 3 major pillars :
# MAGIC  When we create views,mat views,streaming table... we set rules that check data constraints / data quality checks on top of Delta tables , views ,mat views if they fail to pass these rules then we have 3 options
# MAGIC Warn :- we just warn and feed the data into target loc
# MAGIC Drop :- if it fails and we have specifed to drop the records ..it will put the passed data and drop rest 
# MAGIC Fail :- It will simply fail the flow

# COMMAND ----------

# MAGIC %md
# MAGIC # DLT NoteBook Gold Layer

# COMMAND ----------

# create a streaming table on top of all the tables in the Silver layer
# we will use a decorator

# COMMAND ----------

#expectations
looktables_rules = {
    "rule1" : "show_id is NOT NULL",
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)

@dlt.expect_all_or_drop(looktables_rules)
def gold_netflixdirectors():
    df = spark.readStream.format("delta").load(("abfss://silver@sloc.dfs.core.winodows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflixdirectors():
    df = spark.readStream.format("delta").load("abfss://silver@sloc.dfs.core.winodows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdcountries"
)
@dlt.expect_all_or_drop(looktables_rules)
def gold_netflixdirectors():
    df = spark.readStream.format("delta").load("abfss://silver@sloc.dfs.core.winodows.net/netflix_countries")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcategory"
)
@dlt.expect_or_drop("rule1","show_id is NOT NULL")
def gold_netflixdirectors():
    df = spark.readStream.format("delta").load("abfss://silver@sloc.dfs.core.winodows.net/netflix_category")
    return df

# COMMAND ----------

#lets create streaming table 

@dlt.table

def gold_netflixtitles:

    df = spark.readStream.format("delta").load("abfss://silver@sloc.dfs.core.winodows.net/netflix_titles")
    return df


    #dont want to create a direct table .. so we will create a stagging table using this table as source


# COMMAND ----------

@dlt.table 

def gold_stg_netflixtitles():

    df = spark.readStream.format("delta").load("abfss://silver@sloc.dfs.core.windows.net/netflix_titles")
    return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# we can also create views or transformed table
@dlt.table

def gold_trns_netflixtitles:
    df=spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withcolum("newflag", lit(1))
    return df

# COMMAND ----------

#Expectations on Master Data

masterdata_rules = { "rule1" : "newflad is NOT NULL"
                    "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles():

    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")

    return df