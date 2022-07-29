# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType


todays_date = date.today()

year = todays_date.strftime("%Y")
month = todays_date.strftime("%m")
day = todays_date.strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------



# COMMAND ----------

all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('account_key','product_key','status_key','utc_timestamp','application_code')
all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use

sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/e*hidden*/{today}/*.parquet')
sicod = sicod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use
sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
sicod = sicod.where(sicod.application!='SI')
sicod = sicod.select('code_key', 'utc_timestamp', 'code_description_2').dropDuplicates()

# for multi-file use join on key and timestamp, otherwise just key works
all_prod = all_prod.join(sicod, ((all_prod.status_key == sicod.code_key) & (all_prod.utc_timestamp == sicod.utc_timestamp)), how='left')


#filters to business only
bus_cons_lookup = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
all_prod = all_prod.join(bus_cons_lookup, on='product_key', how='left')
all_prod = all_prod.where(all_prod['consumer_or_business_product']=='Business')


all_prod = all_prod.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
all_prod = all_prod.withColumn('merchant_services_flag', when(all_prod['product_key']=="1-55-", lit(1)).otherwise(0))

xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").select('customer_key','account_key', 'relationship_key')
xref = xref.withColumn('customer_key', regexp_replace('customer_key', r'^[0]*', ''))
xref = xref.join(all_prod, on='account_key', how='left')

# COMMAND ----------

display(xref.groupBy('relationship_key').count().orderBy('count'))

# COMMAND ----------

today = "2021/01/04"

all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')#.select('account_key','product_key','status_key','utc_timestamp','application_code')
all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use

sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use
sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
sicod = sicod.where(sicod.application!='SI')
sicod = sicod.select('code_key', 'utc_timestamp', 'code_description_2').dropDuplicates()

# for multi-file use join on key and timestamp, otherwise just key works
all_prod = all_prod.join(sicod, ((all_prod.status_key == sicod.code_key) & (all_prod.utc_timestamp == sicod.utc_timestamp)), how='left')


#filters to business only
bus_cons_lookup = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/2022/05/11/*.parquet') # this needs changed for prod - we should get most recent not some arbitrary date
all_prod = all_prod.join(bus_cons_lookup, on='product_key', how='left')
all_prod = all_prod.where(all_prod['consumer_or_business_product']=='Business')


all_prod = all_prod.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
all_prod = all_prod.withColumn('merchant_services_flag', when(all_prod['product_key']=="1-55-", lit(1)).otherwise(0))

xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").select('customer_key','account_key', 'relationship_key')
xref = xref.withColumn('customer_key', regexp_replace('customer_key', r'^[0]*', ''))
xref = xref.join(all_prod, on='account_key', how='left')

display(xref.groupBy('relationship_key').count().orderBy('count'))

# COMMAND ----------

today = "2019/12/31"

all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/e*hidden*/{today}/*.parquet').select('account_key','product_key','status_key','utc_timestamp','application_code')
all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use

sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use
sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
sicod = sicod.where(sicod.application!='SI')
sicod = sicod.select('code_key', 'utc_timestamp', 'code_description_2').dropDuplicates()

# for multi-file use join on key and timestamp, otherwise just key works
all_prod = all_prod.join(sicod, ((all_prod.status_key == sicod.code_key) & (all_prod.utc_timestamp == sicod.utc_timestamp)), how='left')


#filters to business only
bus_cons_lookup = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/2022/05/11/*.parquet') # same as above don't hardcode dates for prod
all_prod = all_prod.join(bus_cons_lookup, on='product_key', how='left')
all_prod = all_prod.where(all_prod['consumer_or_business_product']=='Business')


all_prod = all_prod.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
all_prod = all_prod.withColumn('merchant_services_flag', when(all_prod['product_key']=="1-55-", lit(1)).otherwise(0))

xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").select('customer_key','account_key', 'relationship_key')
xref = xref.withColumn('customer_key', regexp_replace('customer_key', r'^[0]*', ''))
xref = xref.join(all_prod, on='account_key', how='left')

display(xref.groupBy('relationship_key').count().orderBy('count'))

# COMMAND ----------

today = "2021/01/04"

all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')#.select('account_key','product_key','status_key','utc_timestamp','application_code')
all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use

sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use
sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
sicod = sicod.where(sicod.application!='SI')
sicod = sicod.select('code_key', 'utc_timestamp', 'code_description_2').dropDuplicates()

# for multi-file use join on key and timestamp, otherwise just key works
all_prod = all_prod.join(sicod, ((all_prod.status_key == sicod.code_key) & (all_prod.utc_timestamp == sicod.utc_timestamp)), how='left')


#filters to business only
bus_cons_lookup = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/2022/05/11/*.parquet') # again
all_prod = all_prod.join(bus_cons_lookup, on='product_key', how='left')
all_prod = all_prod.where(all_prod['consumer_or_business_product']=='Business')


all_prod = all_prod.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
all_prod = all_prod.withColumn('merchant_services_flag', when(all_prod['product_key']=="1-55-", lit(1)).otherwise(0))

xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")#.select('customer_key','account_key', 'relationship_key','product_key')
xref = xref.withColumn('customer_key', regexp_replace('customer_key', r'^[0]*', ''))
xref = xref.join(all_prod, on='account_key', how='left')
xref = xref.where(xref['relationship_key']=="1-A-SOL")

display(xref.groupBy('product_key').count().orderBy('count'))

# COMMAND ----------

today = "2019/12/31"

all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')#.select('account_key','product_key','status_key','utc_timestamp','application_code')
all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use

sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use
sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
sicod = sicod.where(sicod.application!='SI')
sicod = sicod.select('code_key', 'utc_timestamp', 'code_description_2').dropDuplicates()

# for multi-file use join on key and timestamp, otherwise just key works
all_prod = all_prod.join(sicod, ((all_prod.status_key == sicod.code_key) & (all_prod.utc_timestamp == sicod.utc_timestamp)), how='left')


#filters to business only
bus_cons_lookup = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/2022/05/11/*.parquet') # once more
all_prod = all_prod.join(bus_cons_lookup, on='product_key', how='left')
all_prod = all_prod.where(all_prod['consumer_or_business_product']=='Business')


all_prod = all_prod.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
all_prod = all_prod.withColumn('merchant_services_flag', when(all_prod['product_key']=="1-55-", lit(1)).otherwise(0))

xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")#.select('customer_key','account_key', 'relationship_key','product_key')
xref = xref.withColumn('customer_key', regexp_replace('customer_key', r'^[0]*', ''))
xref = xref.join(all_prod, on='account_key', how='left')
xref = xref.where(xref['relationship_key']=="1-A-SOL")

display(xref.groupBy('product_key').count().orderBy('count'))

# COMMAND ----------


