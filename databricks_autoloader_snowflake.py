# Databricks notebook source
# MAGIC %md 
# MAGIC ##  Facebook
# MAGIC
# MAGIC
# MAGIC | Tables | 
# MAGIC | --- | 
# MAGIC | analytics_record_v4_facebook_ad | 

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS bronze.facebook_ad
#   (analytics_key string, date date, ad string, ad_name string, adset string, adset_name string, campaign string, campaign_name string, account string, account_name string, advideo string, advideo_name string, metric string, sub_metric string, value decimal(26,8))
# USING delta 
# OPTIONS (path = "s3://vidmob-lakehouse/prod/lakehouse/bronze/facebook/data/facebook_ad")
# TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md **Ad**

# COMMAND ----------

checkpoint_directory = "s3://vidmob-lakehouse/prod/lakehouse/bronze/facebook/checkpoint/ad"
data_source = "s3://vidmob-snowflake-prod-451690304815-us-east-1/stream/snowflake/analytics_record_v4/facebook/ad/"
table_name = "facebook_ad"

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .option("rescuedDataColumn", "_rescue") \
                  .option("delimiter", "\x01") \
                  .option("quote", "")\
                  .option("header", "false")
                  .schema('analytics_key string, date date, ad string, ad_name string, adset string, adset_name string, campaign string, campaign_name string, account string, account_name string, advideo string, advideo_name string, metric string, sub_metric string, value decimal(26,8)')
                  .load(data_source)
                  .writeStream
                  .trigger(availableNow=True)
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .start("s3://vidmob-lakehouse/prod/lakehouse/bronze/facebook/data/facebook_ad")
                  .awaitTermination())
    return query

# COMMAND ----------

# MAGIC %md **Raw data ingestion into bronze table**

# COMMAND ----------

query = autoload_to_table(data_source = data_source,
                          source_format = "csv",
                          table_name = table_name,
                          checkpoint_directory = checkpoint_directory)

# COMMAND ----------

# MAGIC %md ### Snowflake validations

# COMMAND ----------

# Retrieving snowflake credentials from the scope

username = dbutils.secrets.get( scope='vidmob-snowflake', key='username')
password = dbutils.secrets.get( scope='vidmob-snowflake', key='password')

# COMMAND ----------

# snowflake connection options
options = {
  "sfUrl": "https://sfa79964.us-east-1.snowflakecomputing.com",
  "sfUser": username,
  "sfPassword": password,
  "sfDatabase": "ANALYTICS",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "ANALYST"
}

# COMMAND ----------

# Validating with total counts on Snowflake table
analytics_record_v4_facebook_ad = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query", "Select count(*) from analytics_record_v4_facebook_ad") \
  .load()
display(analytics_record_v4_facebook_ad) 
