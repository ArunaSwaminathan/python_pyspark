# Databricks notebook source
# MAGIC %md 
# MAGIC ##  Data Lake Tables - Adwords
# MAGIC
# MAGIC
# MAGIC | adwords | 
# MAGIC | --- | 
# MAGIC | analytics_record_v4_adwords_ad_age | 
# MAGIC | analytics_record_v4_adwords_ad_gender|
# MAGIC | analytics_record_v4_adwords_ad_asset | 

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import *

# COMMAND ----------

# MAGIC %md ### Defining Schema to read raw data from Adwords Platform

# COMMAND ----------

schema = StructType([
        StructField('dimensionSetKey', StringType(), True),
        StructField('network', StringType(), True),
        StructField('ad', StructType([
             StructField('id', StringType(), True),
             StructField('name', StringType(), True)
             ])),
        StructField('adgroup', StructType([
             StructField('id', StringType(), True),
             StructField('name', StringType(), True)
             ])),
        StructField('account', StringType(), True),
        StructField('advideo', StringType(), True),
        StructField('age', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('fieldtype', StringType(), True),
        StructField('metrics', MapType(StringType(), DoubleType())),
        StructField('campaign', StructType([
             StructField('id', StringType(), True),
             StructField('name', StringType(), True)
             ])),
         StructField('date', DateType(), True),
         StructField('importTime', StringType(), True)

         ])


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists lakehouse_temp.adwords_ad_age_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_age_base_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_age_incoming_final;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_gender_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_gender_base_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_gender_incoming_final;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_asset_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_asset_base_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_asset_incoming_final;

# COMMAND ----------

sqlContext.setConf("spark.databricks.adaptive.autoOptimizeShuffle.enabled", 'true')

# COMMAND ----------

newDF = spark.read.format("delta").load("s3://vidmob-lakehouse/prod/lake/Kinesis_raw/")

# COMMAND ----------

import pyspark.sql.functions as f
adwords_raw = newDF.select(col("lake_arrival_date"), col("kinesis_arrival_time"),
     from_json("json", schema).alias("adwords")).where(col("adwords.network") == "adwords")

# COMMAND ----------

import pyspark.sql.functions as f
adwords_raw_new = adwords_raw.select(col("lake_arrival_date"), col("kinesis_arrival_time"), col("adwords")).where(f.col("lake_arrival_date").isin([current_date(), current_date()-1]))

# COMMAND ----------

# MAGIC %md ### Extracting JSON for silver

# COMMAND ----------

from pyspark.sql.functions import isnull, unix_timestamp
adwords_transform = (adwords_raw_new
        .select(col("adwords.ad").getItem("id").alias("ad"),
                col("adwords.ad").getItem("name").alias("ad_name"),
                col("adwords.account").alias("account"),
                lit("null").alias("account_name"),
                col("adwords.advideo").alias("advideo"),
                lit("null").alias("advideo_name"),
                lit("\\N").alias("adnetworktype"),
                lit("null").alias("adnetworktype_name"),
                col("adwords.age").alias("age"),
                lit("null").alias("age_name"),
                col("adwords.gender").alias("gender"),
                lit("null").alias("gender_name"),
                col("adwords.fieldtype").alias("fieldtype"),
                lit("null").alias("fieldtype_name"),
                col("adwords.campaign").getItem("id").alias("campaign"),
                col("adwords.campaign").getItem("name").alias("campaign_name"),
                col("adwords.adgroup").getItem("id").alias("adgroup"),
                col("adwords.adgroup").getItem("name").alias("adgroup_name"),
                explode(col("adwords.metrics")).alias("metric", "value"),
                col("adwords.date").alias("date"),
                col("adwords.dimensionSetKey").alias("dimensionSetKey"),
                to_timestamp(col("adwords.importTime")).alias("import_time")
               
               
               
               
               )
 )

# COMMAND ----------

# MAGIC %md ### adwords raw data transformation

# COMMAND ----------

# MAGIC %md ### ad-age

# COMMAND ----------

ANALYTICS_RECORD_V4_ADWORDS_AD_AGE = adwords_transform.select(col("date"), when(col("adgroup").isNull(), "\\N").otherwise(adwords_transform.adgroup).alias("adgroup"), col("adgroup_name"), when(col("campaign").isNull(), "\\N").otherwise(adwords_transform.campaign).alias("campaign"), col("campaign_name"), when(col("account").isNull(), "\\N").otherwise(adwords_transform.account).alias("account"), col("account_name"), when(col("age").isNull(), "\\N").otherwise(adwords_transform.age).alias("age"), col("age_name"), when(col("adnetworktype").isNull(), "\\N").otherwise(adwords_transform.adnetworktype).alias("adnetworktype"), col("adnetworktype_name"), col("metric"), lit("\\N").alias("sub_metric"), col("value"), col("import_time")).filter(col("dimensionSetKey") == "ad-age")

# COMMAND ----------

# MAGIC %md ### Adding unique identifier - analytics key prior to merge

# COMMAND ----------

ANALYTICS_RECORD_V4_ADWORDS_AD_AGE_FINAL = ANALYTICS_RECORD_V4_ADWORDS_AD_AGE.select(md5(concat(col("adgroup"), lit("-"), col("campaign"), lit("-"), col("account"), lit("-"),  col("age"), lit("-"), col("adnetworktype"), lit("-"), col("metric"), lit("-"), col("sub_metric"))).alias("analytics_key"), md5(concat(col("adgroup"), lit("-"), col("campaign"), lit("-"), col("account"), lit("-"), col("age"), lit("-"), col("adnetworktype"))).alias("akey"), col("date"), col("adgroup"), col("adgroup_name"), col("campaign"), col("campaign_name"), col("account"), col("account_name"), col("age"), col("age_name"), col("adnetworktype"), col("adnetworktype_name"), col("metric"), col("sub_metric"), col("value"), col("import_time"))

# COMMAND ----------

adwords_ad_age_checkpoint = "s3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_age/checkpoint/"

# COMMAND ----------

#Writing data to S3 - delta lake
adwords_ad_age_incoming = ANALYTICS_RECORD_V4_ADWORDS_AD_AGE_FINAL.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("checkpointLocation", adwords_ad_age_checkpoint).save("s3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_age/data/adwords_ad_age_incoming")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.adwords_ad_age_incoming
# MAGIC USING DELTA
# MAGIC LOCATION 's3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_age/data/adwords_ad_age_incoming'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from silver.adwords_ad_age_incoming

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver.adwords_ad_age_incoming AS C1
# MAGIC WHERE EXISTS (SELECT 1
# MAGIC               FROM silver.adwords_ad_age_incoming AS C2
# MAGIC               WHERE C1.analytics_key = C2.analytics_key    
# MAGIC                 AND C1.date   = C2.date    
# MAGIC                 AND C1.import_time   < C2.import_time)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists lakehouse_temp.adwords_ad_age_metric as 
# MAGIC Select analytics_key, akey, date, adgroup, campaign, account, age, adnetworktype, metric, value, value as base_metric
# MAGIC from silver.adwords_ad_age_incoming 
# MAGIC where metric = 'impressions' 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table if not exists lakehouse_temp.adwords_ad_age_base_metric as 
# MAGIC Select a.analytics_key, a.akey, a.date, a.adgroup, a.adgroup_name, a.campaign, a.campaign_name, a.account, a.account_name, a.age, a.age_name, a.adnetworktype, a.adnetworktype_name, a.metric, a.sub_metric, a.value as old_value, b.base_metric
# MAGIC from silver.adwords_ad_age_incoming a
# MAGIC inner join lakehouse_temp.adwords_ad_age_metric b
# MAGIC on a.akey = b.akey and a.date = b.date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table if not exists lakehouse_temp.adwords_ad_age_incoming_final
# MAGIC Select analytics_key, date, adgroup, adgroup_name, campaign, campaign_name, account, account_name, age, age_name, adnetworktype, adnetworktype_name, metric, sub_metric, case when metric in ('active_view_ctr', 'all_conversion_rate', 'average_cpc', 'average_cpe', 'average_cpm', 'average_cpv', 'conversion_rate', 'cost_per_conversion', 'ctr', 'engagement_rate', 'interaction_rate', 'value_per_conversion', 'video_view_rate', 'active_view_cpm') then old_value*base_metric else old_value end as value 
# MAGIC from lakehouse_temp.adwords_ad_age_base_metric

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled=true;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into silver.analytics_record_v4_adwords_ad_age ar
# MAGIC     using (
# MAGIC         select
# MAGIC             ar.analytics_key, ar.date, max(ar.adgroup) as adgroup, max(ar.adgroup_name) as adgroup_name, max(ar.campaign) as campaign, max(ar.campaign_name) as campaign_name, max(ar.account) as account, max(ar.account_name) as account_name, max(ar.age) as age, max(ar.age_name) as age_name, max(ar.adnetworktype) as adnetworktype, max(ar.adnetworktype_name) as adnetworktype_name, max(ar.metric) as metric, max(ar.sub_metric) as sub_metric, max(ar.value) as value
# MAGIC         from lakehouse_temp.adwords_ad_age_incoming_final ar
# MAGIC         group by
# MAGIC             ar.analytics_key, ar.date
# MAGIC     ) ars on ar.analytics_key = ars.analytics_key and ar.date = ars.date
# MAGIC     when matched and ar.value != ars.value then update set value = ars.value
# MAGIC     when not matched then insert (analytics_key, date, adgroup, adgroup_name, campaign, campaign_name, account, account_name, age, age_name, adnetworktype, adnetworktype_name, metric, sub_metric, value) values (ars.analytics_key, ars.date, ars.adgroup, ars.adgroup_name, ars.campaign, ars.campaign_name, ars.account, ars.account_name, ars.age, ars.age_name, ars.adnetworktype, ars.adnetworktype_name, ars.metric, ars.sub_metric, ars.value)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from silver.analytics_record_v4_adwords_ad_age

# COMMAND ----------

# MAGIC %md ### Snowflake validations

# COMMAND ----------

# Retrieving snowflake credentials from the scope

username = dbutils.secrets.get( scope='vidmob-snowflake', key='username')
password = dbutils.secrets.get( scope='vidmob-snowflake', key='password')

# COMMAND ----------

# snowflake connection options
options = {
  "sfUrl": "xxxx",
  "sfUser": username,
  "sfPassword": password,
  "sfDatabase": "xxxx",
  "sfSchema": "xxxx",
  "sfWarehouse": "xxxx"
}

# COMMAND ----------

# Validating with total counts on Snowflake table
analytics_record_v4_adwords_ad_age = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query", "Select count(*) from analytics_record_v4_adwords_ad_age") \
  .load()
display(analytics_record_v4_adwords_ad_age)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists lakehouse_temp.adwords_ad_age_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_age_base_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_age_incoming_final;

# COMMAND ----------

# MAGIC %md ### ad-gender

# COMMAND ----------

import pyspark.sql.functions as f
adwords_ad_gender = adwords_raw_new.select(col("lake_arrival_date"), col("kinesis_arrival_time"), col("adwords")).where(f.col("adwords.dimensionSetKey") == "ad-gender")

# COMMAND ----------

from pyspark.sql.functions import *

adwords_ad_gender_id = adwords_ad_gender.withColumn("adwords_record_id", monotonically_increasing_id())

# COMMAND ----------

from pyspark.sql.functions import isnull, unix_timestamp
adwords_transform_gender = (adwords_ad_gender_id
        .select(col("adwords.ad").getItem("id").alias("ad"),
                col("adwords.ad").getItem("name").alias("ad_name"),
                col("adwords.account").alias("account"),
                lit("null").alias("account_name"),
                col("adwords.advideo").alias("advideo"),
                lit("null").alias("advideo_name"),
                lit("\\N").alias("adnetworktype"),
                lit("null").alias("adnetworktype_name"),
                col("adwords.age").alias("age"),
                lit("null").alias("age_name"),
                col("adwords.gender").alias("gender"),
                lit("null").alias("gender_name"),
                col("adwords.fieldtype").alias("fieldtype"),
                lit("null").alias("fieldtype_name"),
                col("adwords.campaign").getItem("id").alias("campaign"),
                col("adwords.campaign").getItem("name").alias("campaign_name"),
                col("adwords.adgroup").getItem("id").alias("adgroup"),
                col("adwords.adgroup").getItem("name").alias("adgroup_name"),
                explode(col("adwords.metrics")).alias("metric", "value"),
                col("adwords.date").alias("date"),
                col("adwords.dimensionSetKey").alias("dimensionSetKey"),
                col("adwords_record_id"),
                to_timestamp(col("adwords.importTime")).alias("import_time")
               
               
               
               
               )
 )

# COMMAND ----------

ANALYTICS_RECORD_V4_ADWORDS_AD_GENDER = adwords_transform_gender.select(col("date"), when(col("adgroup").isNull(), "\\N").otherwise(adwords_transform_gender.adgroup).alias("adgroup"), col("adgroup_name"), when(col("campaign").isNull(), "\\N").otherwise(adwords_transform_gender.campaign).alias("campaign"), col("campaign_name"), when(col("account").isNull(), "\\N").otherwise(adwords_transform_gender.account).alias("account"), col("account_name"), when(col("gender").isNull(), "\\N").otherwise(adwords_transform_gender.gender).alias("gender"), col("gender_name"), when(col("adnetworktype").isNull(), "\\N").otherwise(adwords_transform_gender.adnetworktype).alias("adnetworktype"), col("adnetworktype_name"), col("metric"), lit("\\N").alias("sub_metric"), col("value"), col("adwords_record_id"), col("import_time")).filter(col("dimensionSetKey") == "ad-gender")
# display(ANALYTICS_RECORD_V4_ADWORDS_AD_GENDER)

# COMMAND ----------

# MAGIC %md ### Adding unique identifier - analytics key prior to merge

# COMMAND ----------

ANALYTICS_RECORD_V4_ADWORDS_AD_GENDER_FINAL = ANALYTICS_RECORD_V4_ADWORDS_AD_GENDER.select(md5(concat(col("adgroup"), lit("-"), col("campaign"), lit("-"), col("account"), lit("-"),  col("gender"), lit("-"), col("adnetworktype"), lit("-"), col("metric"), lit("-"), col("sub_metric"))).alias("analytics_key"), md5(concat(col("adgroup"), lit("-"), col("campaign"), lit("-"), col("account"), lit("-"), col("gender"), lit("-"), col("adnetworktype"))).alias("akey"), col("date"), col("adgroup"), col("adgroup_name"), col("campaign"), col("campaign_name"), col("account"), col("account_name"), col("gender"), col("gender_name"), col("adnetworktype"), col("adnetworktype_name"), col("metric"), col("sub_metric"), col("value"), col("import_time"), col("adwords_record_id"))
# display(ANALYTICS_RECORD_V4_ADWORDS_AD_GENDER_FINAL)

# COMMAND ----------

adwords_ad_gender_checkpoint = "s3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_gender/checkpoint/"

# COMMAND ----------

# Writing data to S3 - delta lake
adwords_ad_gender_incoming = ANALYTICS_RECORD_V4_ADWORDS_AD_GENDER_FINAL.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("checkpointLocation", adwords_ad_gender_checkpoint).save("s3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_gender/data/adwords_ad_gender_incoming")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.adwords_ad_gender_incoming
# MAGIC USING DELTA
# MAGIC LOCATION 's3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_gender/data/adwords_ad_gender_incoming'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from silver.adwords_ad_gender_incoming

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver.adwords_ad_gender_incoming AS C1
# MAGIC WHERE EXISTS (SELECT 1
# MAGIC               FROM silver.adwords_ad_gender_incoming AS C2
# MAGIC               WHERE C1.analytics_key = C2.analytics_key    
# MAGIC                 AND C1.date   = C2.date    
# MAGIC                 AND C1.import_time   < C2.import_time)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table lakehouse_temp.adwords_ad_gender_metric as 
# MAGIC Select analytics_key, akey, date, adwords_record_id, adgroup, campaign, account, gender, adnetworktype, metric, value, value as base_metric
# MAGIC from silver.adwords_ad_gender_incoming 
# MAGIC where metric = 'impressions' 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM lakehouse_temp.adwords_ad_gender_metric AS C1
# MAGIC WHERE EXISTS
# MAGIC (SELECT 1
# MAGIC     FROM lakehouse_temp.adwords_ad_gender_metric AS C2
# MAGIC     WHERE C1.akey = C2.akey    
# MAGIC                 AND C1.date   = C2.date 
# MAGIC                 AND C1.value   < C2.value)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table lakehouse_temp.adwords_ad_gender_base_metric as 
# MAGIC Select a.analytics_key, a.akey, a.adwords_record_id, a.date, a.adgroup, a.adgroup_name, a.campaign, a.campaign_name, a.account, a.account_name, a.gender, a.gender_name, a.adnetworktype, a.adnetworktype_name, a.metric, a.sub_metric, a.value as old_value, b.base_metric
# MAGIC from silver.adwords_ad_gender_incoming a
# MAGIC inner join lakehouse_temp.adwords_ad_gender_metric b
# MAGIC on a.akey = b.akey and a.adwords_record_id = b.adwords_record_id and a.date = b.date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table lakehouse_temp.adwords_ad_gender_incoming_final
# MAGIC Select analytics_key, date, adgroup, adgroup_name, campaign, campaign_name, account, account_name, gender, gender_name, adnetworktype, adnetworktype_name, metric, sub_metric, case when metric in ('active_view_ctr', 'all_conversion_rate', 'average_cpc', 'average_cpe', 'average_cpm', 'average_cpv', 'conversion_rate', 'cost_per_conversion', 'ctr', 'engagement_rate', 'interaction_rate', 'value_per_conversion', 'video_view_rate', 'active_view_cpm') then old_value*base_metric else old_value end as value 
# MAGIC from lakehouse_temp.adwords_ad_gender_base_metric

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled=true;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into silver.analytics_record_v4_adwords_ad_gender ar
# MAGIC     using (
# MAGIC         select
# MAGIC             ar.analytics_key, ar.date, max(ar.adgroup) as adgroup, max(ar.adgroup_name) as adgroup_name, max(ar.campaign) as campaign, max(ar.campaign_name) as campaign_name, max(ar.account) as account, max(ar.account_name) as account_name, max(ar.gender) as gender, max(ar.gender_name) as gender_name, max(ar.adnetworktype) as adnetworktype, max(ar.adnetworktype_name) as adnetworktype_name, max(ar.metric) as metric, max(ar.sub_metric) as sub_metric, max(ar.value) as value
# MAGIC         from lakehouse_temp.adwords_ad_gender_incoming_final ar
# MAGIC         group by
# MAGIC             ar.analytics_key, ar.date
# MAGIC     ) ars on ar.analytics_key = ars.analytics_key and ar.date = ars.date
# MAGIC     when matched and ar.value != ars.value then update set value = ars.value
# MAGIC     when not matched then insert (analytics_key, date, adgroup, adgroup_name, campaign, campaign_name, account, account_name, gender, gender_name, adnetworktype, adnetworktype_name, metric, sub_metric, value) values (ars.analytics_key, ars.date, ars.adgroup, ars.adgroup_name, ars.campaign, ars.campaign_name, ars.account, ars.account_name, ars.gender, ars.gender_name, ars.adnetworktype, ars.adnetworktype_name, ars.metric, ars.sub_metric, ars.value)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from silver.analytics_record_v4_adwords_ad_gender

# COMMAND ----------

# MAGIC %md ### Snowflake validations

# COMMAND ----------

# Validating with total counts on Snowflake table
analytics_record_v4_adwords_ad_gender = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query", "Select count(*) from analytics_record_v4_adwords_ad_gender") \
  .load()
display(analytics_record_v4_adwords_ad_gender)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists lakehouse_temp.adwords_ad_gender_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_gender_base_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_gender_incoming_final;

# COMMAND ----------

# MAGIC %md ### ad-asset

# COMMAND ----------

ANALYTICS_RECORD_V4_ADWORDS_AD_ASSET = adwords_transform.select(col("date"), when(col("advideo").isNull(), "\\N").otherwise(adwords_transform.advideo).alias("advideo"), col("advideo_name"), when(col("ad").isNull(), "\\N").otherwise(adwords_transform.ad).alias("ad"), col("ad_name"), when(col("adgroup").isNull(), "\\N").otherwise(adwords_transform.adgroup).alias("adgroup"), col("adgroup_name"), when(col("campaign").isNull(), "\\N").otherwise(adwords_transform.campaign).alias("campaign"), col("campaign_name"), when(col("account").isNull(), "\\N").otherwise(adwords_transform.account).alias("account"), col("account_name"), when(col("fieldtype").isNull(), "\\N").otherwise(adwords_transform.fieldtype).alias("fieldtype"), col("fieldtype_name"), col("metric"), lit("\\N").alias("sub_metric"), col("value"), col("import_time")).filter(col("dimensionSetKey") == "ad-asset")

# COMMAND ----------

# MAGIC %md ### Adding unique identifier - analytics key prior to merge

# COMMAND ----------

ANALYTICS_RECORD_V4_ADWORDS_AD_ASSET_FINAL = ANALYTICS_RECORD_V4_ADWORDS_AD_ASSET.select(md5(concat(col("advideo"), lit("-"), col("ad"), lit("-"), col("adgroup"), lit("-"), col("campaign"), lit("-"), col("account"), lit("-"),  col("fieldtype"), lit("-"), col("metric"), lit("-"), col("sub_metric"))).alias("analytics_key"), md5(concat(col("advideo"), lit("-"), col("ad"), lit("-"), col("adgroup"), lit("-"), col("campaign"), lit("-"), col("account"), lit("-"), col("fieldtype"))).alias("akey"), col("date"), col("advideo"), col("advideo_name"), col("ad"), col("ad_name"), col("adgroup"), col("adgroup_name"), col("campaign"), col("campaign_name"), col("account"), col("account_name"), col("fieldtype"), col("fieldtype_name"), col("metric"), col("sub_metric"), col("value"), col("import_time"))

# COMMAND ----------

adwords_ad_asset_checkpoint = "s3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_dma/checkpoint/"

# COMMAND ----------

#Writing data to S3 - delta lake
adwords_ad_asset_incoming = ANALYTICS_RECORD_V4_ADWORDS_AD_ASSET_FINAL.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("checkpointLocation", adwords_ad_asset_checkpoint).save("s3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_asset/data/adwords_ad_asset_incoming")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.adwords_ad_asset_incoming
# MAGIC USING DELTA
# MAGIC LOCATION 's3://vidmob-lakehouse/prod/lakehouse/silver/adwords/ad_asset/data/adwords_ad_asset_incoming'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from silver.adwords_ad_asset_incoming

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver.adwords_ad_asset_incoming AS C1
# MAGIC WHERE EXISTS (SELECT 1
# MAGIC               FROM silver.adwords_ad_asset_incoming AS C2
# MAGIC               WHERE C1.analytics_key = C2.analytics_key    
# MAGIC                 AND C1.date   = C2.date    
# MAGIC                 AND C1.import_time   < C2.import_time)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists lakehouse_temp.adwords_ad_asset_metric as 
# MAGIC Select analytics_key, akey, date, advideo, ad, adgroup, campaign, account, fieldtype, metric, value, value as base_metric
# MAGIC from silver.adwords_ad_asset_incoming 
# MAGIC where metric = 'impressions' 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table if not exists lakehouse_temp.adwords_ad_asset_base_metric as 
# MAGIC Select a.analytics_key, a.akey, a.date, a.advideo, a.advideo_name, a.ad, a.ad_name, a.adgroup, a.adgroup_name, a.campaign, a.campaign_name, a.account, a.account_name, a.fieldtype, a.fieldtype_name, a.metric, a.sub_metric, a.value as old_value, b.base_metric
# MAGIC from silver.adwords_ad_asset_incoming a
# MAGIC inner join lakehouse_temp.adwords_ad_asset_metric b
# MAGIC on a.akey = b.akey and a.date = b.date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table if not exists lakehouse_temp.adwords_ad_asset_incoming_final
# MAGIC Select analytics_key, date, advideo, advideo_name, ad, ad_name, adgroup, adgroup_name, campaign, campaign_name, account, account_name, fieldtype, fieldtype_name, metric, sub_metric, case when metric in ('active_view_ctr', 'all_conversion_rate', 'average_cpc', 'average_cpe', 'average_cpm', 'average_cpv', 'conversion_rate', 'cost_per_conversion', 'ctr', 'engagement_rate', 'interaction_rate', 'value_per_conversion', 'video_view_rate', 'active_view_cpm') then old_value*base_metric else old_value end as value 
# MAGIC from lakehouse_temp.adwords_ad_asset_base_metric

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled=true;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into silver.analytics_record_v4_adwords_ad_asset ar
# MAGIC     using (
# MAGIC         select
# MAGIC             ar.analytics_key, ar.date, max(ar.advideo) as advideo, max(ar.advideo_name) as advideo_name, max(ar.ad) as ad, max(ar.ad_name) as ad_name, max(ar.adgroup) as adgroup, max(ar.adgroup_name) as adgroup_name, max(ar.campaign) as campaign, max(ar.campaign_name) as campaign_name, max(ar.account) as account, max(ar.account_name) as account_name, max(ar.fieldtype) as fieldtype, max(ar.fieldtype_name) as fieldtype_name, max(ar.metric) as metric, max(ar.sub_metric) as sub_metric, max(ar.value) as value
# MAGIC         from lakehouse_temp.adwords_ad_asset_incoming_final ar
# MAGIC         group by
# MAGIC             ar.analytics_key, ar.date
# MAGIC     ) ars on ar.analytics_key = ars.analytics_key and ar.date = ars.date
# MAGIC     when matched and ar.value != ars.value then update set value = ars.value
# MAGIC     when not matched then insert (analytics_key, date, advideo, advideo_name, ad, ad_name, adgroup, adgroup_name, campaign, campaign_name, account, account_name, fieldtype, fieldtype_name, metric, sub_metric, value) values (ars.analytics_key, ars.date, ars.advideo, ars.advideo_name, ars.ad, ars.ad_name, ars.adgroup, ars.adgroup_name, ars.campaign, ars.campaign_name, ars.account, ars.account_name, ars.fieldtype, ars.fieldtype_name, ars.metric, ars.sub_metric, ars.value)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*) from silver.analytics_record_v4_adwords_ad_asset

# COMMAND ----------

# MAGIC %md ### Snowflake validations

# COMMAND ----------

# Validating with total counts on Snowflake table
analytics_record_v4_adwords_ad_asset = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("query", "Select count(*) from analytics_record_v4_adwords_ad_asset") \
  .load()
display(analytics_record_v4_adwords_ad_asset)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists lakehouse_temp.adwords_ad_asset_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_asset_base_metric;
# MAGIC drop table if exists lakehouse_temp.adwords_ad_asset_incoming_final;
