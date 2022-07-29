# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType

# COMMAND ----------

date_df= spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')

date_df = date_df.select('utc_timestamp').dropDuplicates()

date_df = date_df.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

date_df = date_df.orderBy('utc_timestamp')

date_list = date_df.rdd.map(lambda x: x[0]).collect()

date_list = date_list[-2:]

# COMMAND ----------

if date.today() != date_list[-1]:
    print('no current file')
else:
    todays_date = date_list[-1]
    prior = date_list[-2]
    
    year = todays_date.strftime("%Y")
    month = todays_date.strftime("%m")
    day = todays_date.strftime("%d")
    today = "".join(str(x) for x in year + "/" + month + "/" + day)
    
    customers = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
    customers = customers.withColumn('most_recent_acquisition_date', when(customers['most_recent_acquisition_date'].isNull(), customers['original_acquisition_date']).otherwise(customers['most_recent_acquisition_date']))
    customers = customers.withColumn('loss_length', datediff(customers['most_recent_acquisition_date'],customers['most_recent_lost_date']))
    customers = customers.withColumn('fraud_flag', when((customers['loss_length']<5)&(customers['loss_length']>=0), "yes").otherwise("no"))
    customers = customers.where(customers['fraud_flag']=='no')
    
    new_cust = customers.where(customers['original_acquisition_date'] == customers['utc_timestamp'])
    new_cust = new_cust.withColumn('new_customer_flag', lit(1))
    new_cust = new_cust.select('utc_timestamp','customer_key','new_customer_flag')
    
    returning_cust = customers.where((customers['most_recent_acquisition_date'] == customers['utc_timestamp']) & (customers['most_recent_acquisition_date'] != customers['original_acquisition_date']))
    returning_cust = returning_cust.where(returning_cust['loss_length']>180)
    returning_cust = returning_cust.withColumn('returning_customer_flag', lit(1))
    returning_cust = returning_cust.select('utc_timestamp','customer_key','returning_customer_flag')
    
    lost_cust = customers.where(customers['most_recent_lost_date'] == lit(prior))
    lost_cust = lost_cust.withColumn('lost_customer_flag',when(lost_cust['most_recent_acquisition_date'] == lost_cust['original_acquisition_date'], lit(1)))
    lost_cust = lost_cust.withColumn('relost_customer_flag', when(lost_cust['most_recent_acquisition_date'] != lost_cust['original_acquisition_date'], lit(1)))
    lost_cust = lost_cust.select('utc_timestamp','customer_key','lost_customer_flag','most_recent_acquisition_date', 'relost_customer_flag')
    
    customer_df = new_cust.unionByName(returning_cust, allowMissingColumns =True).unionByName(lost_cust, allowMissingColumns =True)
    
    
     branch = spark.table('dynamic_branch_assignments.dynamic_branch_assignments')
        customer_df = customer_df.join(branch, on = ['customer_key', 'utc_timestamp'], how='left')

        customer_df = customer_df.select('utc_timestamp','customer_key','new_customer_flag','returning_customer_flag','lost_customer_flag','relost_customer_flag','most_recent_acquisition_date','dynamic_branch_name','dynamic_branch_id')
    customer_df = customer_df.select('utc_timestamp', 'customer_key', 'new_customer_flag', 'returning_customer_flag', 'lost_customer_flag', 'relost_customer_flag', 'most_recent_acquisition_date')
    
    customer_df.createOrReplaceTempView('temp_df')
    
    spark.sql("""
    merge into power_bi_data_products.retail_new_customers_3 sink
    using temp_df source
    on source.utc_timestamp = sink.utc_timestamp
    when matched then update set *
    when not matched then insert *""")

# COMMAND ----------


