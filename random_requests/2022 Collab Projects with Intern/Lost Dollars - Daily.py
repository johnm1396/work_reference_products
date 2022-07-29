# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dateutil.relativedelta import relativedelta
import datetime
import calendar

# COMMAND ----------

date_df= spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/customer_acquisition_dates/*/*/*/*.parquet')

date_df = date_df.select('utc_timestamp').dropDuplicates()

date_df = date_df.orderBy('utc_timestamp')

date_list = date_df.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

if datetime.date.today() != date_list[-1]:
    print('no current file')
else:
    current = date_list[-1]
    two_months_ago = current + relativedelta(months=-2)
    
    date_list_filtered = []
    
    for date in date_list: 
        if date <= two_months_ago:
            date_list_filtered.append(date)
    
    if len(date_list_filtered) > 0:
        prior = date_list_filtered[-1]
        
        year = prior.strftime("%Y")
        month = prior.strftime("%m")
        day = prior.strftime("%d")
        prior_file = "".join(str(x) for x in year + "/" + month + "/" + day)
        
        customer = spark.table('power_bi_data_products.retail_new_customers_3')
        customer_lost = customer.where(customer.lost_customer_flag == 1)
        customer_lost = customer_lost.where(customer_lost.utc_timestamp == lit(current))
        
        xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{prior_file}/*.parquet").select('customer_key','account_key','primary_customer_flag')
        xref = xref.where(xref['primary_customer_flag']=='Y')
        
        customer_lost = customer_lost.join(xref, on='customer_key', how='left')
        
        dd_master = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{prior_file}/*.parquet').select('account_key','average_collected_balance_last_12_months')
        
        customer_lost = customer_lost.join(dd_master, on='account_key', how='left')
        
        customer_lost = customer_lost.groupBy('utc_timestamp').agg(sum('average_collected_balance_last_12_months').alias('deposit_dollars_lost'))
        
        customer_lost.createOrReplaceTempView('temp_df')
        
        spark.sql("""
        merge into power_bi_data_products.lost_customer_dollars sink
        using temp_df source
        on source.utc_timestamp = sink.utc_timestamp
        when matched then update set *
        when not matched then insert *""")

# COMMAND ----------


