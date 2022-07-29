# Databricks notebook source
# package imports, shocking... I know.
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC DAILY PROCESSING CODE GOING FORWARD

# COMMAND ----------

# read in rm_master to get historical processing dates
date_df = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")
date_df = date_df.select('utc_timestamp').dropDuplicates()

# fix the malformed dates
date_df = date_df.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

# order em
date_df = date_df.orderBy('utc_timestamp')

# make a list because lists are awesome
date_list = date_df.rdd.map(lambda x: x[0]).collect()

# we only care about the most recent two processing dates
date_list = date_list[-2:]

# COMMAND ----------

if date.today() != date_list[-1]:
    print('no current file')
else:
    # date variables for current and prior day
    current = date_list[-1]
    prior =  date_list[-2]
    
    # date constructs for filepaths
    year = current.strftime("%Y")
    month = current.strftime("%m")
    day = current.strftime("%d")
    current = "".join(str(x) for x in year + "/" + month + "/" + day)
    
    year = prior.strftime("%Y")
    month = prior.strftime("%m")
    day = prior.strftime("%d")
    prior = "".join(str(x) for x in year + "/" + month + "/" + day)
    
    # prior day stuff
    # read in xref, fix utc_timestamp, cast customer key to remove stupid zeros
    xref_prior = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{prior}/*.parquet")
    xref_prior = xref_prior.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))
    xref_prior = xref_prior.withColumn('customer_key', xref_prior.customer_key.cast(IntegerType()))
    
    # read in fi_core_product, rename top to product_group, select app_code and product_group, drop duplicates
    product_prior = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{prior}/*.parquet").withColumnRenamed('top', 'product_group').select('application_code', 'product_group').dropDuplicates()
    
    # join product_group to xref to get product_group
    xref_prior = xref_prior.join(product_prior, xref_prior.application == product_prior.application_code, 'left')
    
    # read in fi_core_relationship, select relationship_key and ownership_type, drop duplicates
    rmrel_prior = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{prior}/*.parquet').select('relationship_key', 'ownership_type').dropDuplicates()
    
    # join rmrel to xref to get ownership_type
    xref_prior = xref_prior.join(rmrel_prior, 'relationship_key', 'left')
    
    # read in allprod, select acct_key date_key product_key and status_key
    all_prod_prior = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{prior}/*.parquet').select('account_key', 'date_key', 'product_key', 'status_key')
    
    # read in sicod1 and do a bunch of stuff to get conformed_status
    sicod_prior = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{prior}/*.parquet')
    sicod_prior = sicod_prior.where((sicod_prior.code_type=='STAT')|(sicod_prior.code_type=='SS')|(sicod_prior.code_type=='LNST')|(sicod_prior.code_type=='CLSD'))
    sicod_prior = sicod_prior.where(sicod_prior.application!='SI')
    sicod_prior = sicod_prior.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
    sicod_prior = sicod_prior.select('code_key', 'conformed_status')
    sicod_prior = sicod_prior.dropDuplicates()
    
    # join sicod to ap and complete conformed_status calc with a fillna()
    all_prod_prior = all_prod_prior.join(sicod_prior, all_prod_prior.status_key == sicod_prior.code_key, 'left').select('account_key', 'conformed_status').fillna('OPEN')
    
    # join ap to xref
    xref_prior = xref_prior.join(all_prod_prior, on='account_key', how='left')
    
    # filter to customers with direct relationships to a product that is open or dormant
    xref_prior = xref_prior.where((xref_prior.ownership_type == 'Direct') & (xref_prior.product_group != 'Services') & ((xref_prior.conformed_status == 'OPEN') | (xref_prior.conformed_status == 'DORMANT')))
    
    # select only needed columns
    xref_prior = xref_prior.select('customer_key', 'utc_timestamp').dropDuplicates()
    
    # current day stuff
    # read in xref, fix utc_timestamp, cast customer key to remove stupid zeros
    xref_current = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{current}/*.parquet")
    xref_current = xref_current.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))
    xref_current = xref_current.withColumn('customer_key', xref_current.customer_key.cast(IntegerType()))
    
    # read in fi_core_product, rename top to product_group, select app_code and product_group, drop duplicates
    product_current = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{current}/*.parquet").withColumnRenamed('top', 'product_group').select('application_code', 'product_group').dropDuplicates()
    
    # join product_group to xref to get product_group
    xref_current = xref_current.join(product_current, xref_current.application == product_current.application_code, 'left')
    
    # read in fi_core_relationship, select relationship_key and ownership_type, drop duplicates
    rmrel_current = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{current}/*.parquet').select('relationship_key', 'ownership_type').dropDuplicates()
    
    # join rmrel to xref to get ownership_type
    xref_current = xref_current.join(rmrel_current, 'relationship_key', 'left')
    
    # read in allprod, select acct_key date_key product_key and status_key
    all_prod_current = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{current}/*.parquet').select('account_key', 'date_key', 'product_key', 'status_key')
    
    # read in sicod1 and do a bunch of stuff to get conformed_status
    sicod_current = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{current}/*.parquet')
    sicod_current = sicod_current.where((sicod_current.code_type=='STAT')|(sicod_current.code_type=='SS')|(sicod_current.code_type=='LNST')|(sicod_current.code_type=='CLSD'))
    sicod_current = sicod_current.where(sicod_current.application!='SI')
    sicod_current = sicod_current.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
    sicod_current = sicod_current.select('code_key', 'conformed_status')
    sicod_current = sicod_current.dropDuplicates()
    
    # join sicod to ap and complete conformed_status calc with a fillna()
    all_prod_current = all_prod_current.join(sicod_current, all_prod_current.status_key == sicod_current.code_key, 'left').select('account_key', 'conformed_status').fillna('OPEN')
    
    # join ap to xref
    xref_current = xref_current.join(all_prod_current, on='account_key', how='left')
    
    # filter to customers with direct relationships to a product that is open or dormant
    xref_current = xref_current.where((xref_current.ownership_type == 'Direct') & (xref_current.product_group != 'Services') & ((xref_current.conformed_status == 'OPEN') | (xref_current.conformed_status == 'DORMANT')))
    
    # select only needed columns
    xref_current = xref_current.select('customer_key', 'utc_timestamp').dropDuplicates()
    
    # get processing days lost and acquired customers
    lost_today = xref_prior.join(xref_current, on='customer_key', how='leftanti')
    acquired_today = xref_current.join(xref_prior, on='customer_key', how='leftanti')
    
    # process daily lost
    lost_today.createOrReplaceTempView('lost_today')
    
    spark.sql("""
    merge into customer_acquisition_dates.acquisition_dates sink
    using lost_today source
    on source.customer_key = sink.customer_key
    when matched then update set sink.most_recent_lost_date = source.utc_timestamp""")
    
    # process daily acquired
    acquired_today.createOrReplaceTempView('acquired_today')
    
    spark.sql("""
    merge into customer_acquisition_dates.acquisition_dates sink
    using acquired_today source
    on source.customer_key = sink.customer_key
    when matched then update set sink.most_recent_acquisition_date = source.utc_timestamp
    when not matched then insert (sink.customer_key, sink.original_acquisition_date, sink.most_recent_acquisition_date) values (source.customer_key, source.utc_timestamp, source.utc_timestamp)""")
    
    # read in the delta table and write it to parquet for historical records
    df = spark.table('customer_acquisition_dates.acquisition_dates')
    df = df.withColumn('utc_timestamp', lit(date_list[-1]))
    df.coalesce(1).write.mode("overwrite").parquet(f"/mnt/*hidden*/{current}/")

# COMMAND ----------

# check the counts make sense (should always be increasing)
df_validate = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')
display(df_validate.groupBy('utc_timestamp').count().orderBy('utc_timestamp', ascending=False))

# COMMAND ----------


