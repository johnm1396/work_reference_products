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

# get all all_prod files
all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet').select('account_key', 'date_opened', 'date_closed', 'application_code', 'sub_application_code', 'gl_type', 'gl_type_key', 'opening_amount', 'current_balance', 'average_balance', 'product_key', 'utc_timestamp')

# fix processing timestamp to make them uniform
all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

# get most recent version of each account from files
all_prod = all_prod.orderBy('utc_timestamp', ascending=False).dropDuplicates(subset=['account_key'])

# filter to deposit products
all_prod = all_prod.where(all_prod.application_code.isin('DD', 'SV', 'CD', 'IR'))

# filter to accounts opened during requested timeframe
all_prod = all_prod.where(all_prod['date_opened'] >= date(2019,1,1))

# get most current b/c lookup
bus_cons_lookup = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('product_key', 'consumer_or_business_product')

# join to all_prod on product_key
all_prod = all_prod.join(bus_cons_lookup, on='product_key', how='left')

# filter to consumer products
all_prod = all_prod.where(all_prod.consumer_or_business_product == 'Consumer')

# get most recent gltype
v_ods_gltype = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('gl_type_code', 'application_code', 'gl_type_description')

# join to all_prod
all_prod = all_prod.join(v_ods_gltype, ((all_prod.gl_type == v_ods_gltype.gl_type_code) & (all_prod.sub_application_code == v_ods_gltype.application_code)), how='left')

# filter to retail
all_prod = all_prod.where(all_prod.gl_type_description.startswith('RET'))

# pivot to get aggregates required
all_prod_agg = all_prod.withColumn('year', date_format(all_prod.date_opened, 'yyyy'))
all_prod_agg = all_prod_agg.withColumn('month', date_format(all_prod_agg.date_opened, 'MM'))
all_prod_agg = all_prod_agg.withColumnRenamed('sub_application_code', 'product_type')
all_prod_agg = all_prod_agg.groupBy(['year', 'month', 'product_type']).agg(countDistinct('account_key').alias('number_of_new_accounts'), sum('opening_amount').alias('total_new_deposit_dollars'))

# COMMAND ----------

display(all_prod_agg.orderBy(['year', 'month', 'product_type'], ascending = True))

# COMMAND ----------


