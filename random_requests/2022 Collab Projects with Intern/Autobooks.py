# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

year = date.today().strftime("%Y")
month = date.today().strftime("%m")
day = date.today().strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

# gets customer_key, account_key, and conformed_status, and product_key for all accounts
all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('account_key','product_key','status_key','utc_timestamp','application_code')
all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end""")) # for multi-file use

sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
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

# gets list of customers using beb accounts
beb = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('cis_number').dropDuplicates()

# gets list of all open accounts associated with those customers
beb = beb.join(xref, xref['customer_key']==beb['cis_number'], how='left')
beb = beb.where(beb['conformed_status']=='OPEN')

# COMMAND ----------

# creates flag for each desired transaction type
trans = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/*/*/*/*.parquet")
trans = trans.withColumnRenamed("db/cr_flag", "db_cr_flag")
trans = trans.withColumn('description', lower(trans['description']))
trans = trans.where(trans['db_cr_flag']=="C")
trans = trans.where(trans['original_process_date']>date(2021,5,11))
trans = trans.withColumn('paypal_flag',when(trans['description'].contains('paypal'), lit(1)).otherwise(0))
trans = trans.withColumn('zelle_flag',when(trans['description'].contains('zelle'), lit(1)).otherwise(0))
trans = trans.withColumn('venmo_flag',when(trans['description'].contains('venmo'), lit(1)).otherwise(0))
trans = trans.withColumn('cashapp_flag',when(trans['description'].contains('cash app'), lit(1)).otherwise(0))
trans = trans.withColumn('stripe_flag',when((trans['description'].contains('stripe/'))|(trans['description'].contains('/stripe')| (trans['description'].contains('stripe transfer'))), lit(1)).otherwise(0))
trans = trans.withColumn('square_flag', when((trans['description'].contains('square inc'))&~(trans['description'].contains('cash app')), lit(1)).otherwise(0))                                                                  

# COMMAND ----------

beb = beb.join(trans, on='account_key', how='left')
beb = beb.fillna(0, subset=['paypal_flag','zelle_flag','venmo_flag','cashapp_flag','stripe_flag', 'square_flag'])
beb = beb.groupBy('cis_number').agg(sum('paypal_flag').alias('paypal_credits'),sum('zelle_flag').alias('zelle_credits'),sum('venmo_flag').alias('venmo_credits'),sum('cashapp_flag').alias('cashapp_credits'), sum('stripe_flag').alias('stripe_credits'),sum('square_flag').alias('square_credits'), sum('merchant_services_flag').alias('merchant_services_flag'))

# filters out customers who have merchant services
# beb = beb.where(beb['merchant_services_flag']==0)


beb = beb.withColumn('has_credit_from_select_merchants', when((beb['paypal_credits']+beb['zelle_credits']+beb['venmo_credits']+beb['cashapp_credits']+beb['stripe_credits']+beb['square_credits'])>0, 'yes').otherwise('no'))

# COMMAND ----------

display(beb)

# COMMAND ----------

