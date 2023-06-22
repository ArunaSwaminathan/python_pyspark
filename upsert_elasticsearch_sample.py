# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

PAGINATION_LIMIT = 20000

hostname = "xxxx"
port = 443
user = "xxxx"
password = "xxxx"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from creative_analytics_prod.aepr.allplatforms_media_and_tag_final where network = 'instagrampage'

# COMMAND ----------

def split_df(df, limit = PAGINATION_LIMIT):
    total = df.count()
    if total <= limit:
        return [df]
    
    num_chunks = total//limit
    num_chunks = num_chunks +1
    chunk_percent = 1/num_chunks
    return df.randomSplit([chunk_percent]*num_chunks)
    
def upsert_platform(network, limit = PAGINATION_LIMIT, offset_chunk = 0):
    index = "media-filtering-{}".format(network)
    sql_query = "select * from creative_analytics_prod.aepr.allplatforms_media_and_tag_final where network = '{}'".format(network)
    print(index)
    print(sql_query)
    df_rec = spark.sql(sql_query)
    total = df_rec.count()
    df_splitted = split_df(df_rec, limit)
    print('Platform {} total: {}, chunks {}'.format(network, total, len(df_splitted)))
    
    for idx, df in enumerate(df_splitted):
        print(df.count())
        if offset_chunk <= idx:
            ( df.write
              .format( "org.elasticsearch.spark.sql" )
              .option( "es.nodes",   hostname )
              .option("es.net.http.auth.user",   user)
              .option("es.net.http.auth.pass",   password)
              .option( "es.port",    port     )
              .option( "es.net.ssl", "true"      )
              .option("es.mapping.id", "unq_id")
              .option( "es.nodes.wan.only", "true" )
              .option("es.write.operation", "upsert")
              .mode( "append" )
              .save( f"{index}" )
            )
            print('Finished sync {} at limit {}, chunk #{} (total {})'.format(network, limit, idx, total))
        else:
            print('skipping idx {}'.format(idx))
        

# COMMAND ----------

upsert_platform('instagrampage')

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC Thread.sleep(600000)

# COMMAND ----------

upsert_platform('instagrampage')
