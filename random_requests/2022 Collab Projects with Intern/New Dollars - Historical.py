# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from dateutil.relativedelta import relativedelta
import datetime
import calendar

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS power_bi_data_products.new_customer_dollars (utc_timestamp DATE, deposit_dollars_new BIGINT)
# MAGIC USING DELTA

# COMMAND ----------

date_df= spark.table('power_bi_data_products.retail_new_customers_3')

date_df = date_df.select('utc_timestamp').dropDuplicates()

date_df = date_df.orderBy('utc_timestamp')
date_df = date_df.where(date_df.utc_timestamp >= datetime.date(2021,2,1))
date_list = date_df.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

#the most recent two months are no good
for index, item in enumerate(date_list):
    
    date_list_filtered = []
    two_months_later = item + relativedelta(months=2)
    me_two_months_later = two_months_later.replace(day = calendar.monthrange(two_months_later.year, two_months_later.month)[1])
    
    for date in date_list:
        if date <= me_two_months_later:
            date_list_filtered.append(date)
    
    if len(date_list_filtered) > 0:
        processing_date = item
        future_date = date_list_filtered[-1]
    
        year = future_date.strftime("%Y")
        month = future_date.strftime("%m")
        day = future_date.strftime("%d")
        future = "".join(str(x) for x in year + "/" + month + "/" + day)

        customer = spark.table('power_bi_data_products.retail_new_customers_3')
        customer_new = customer.where(customer['new_customer_flag']==1)
        customer_new = customer_new.where(customer_new['utc_timestamp']==lit(processing_date))

        xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{future}/*.parquet").select('customer_key','account_key', 'relationship_key','primary_customer_flag')
        xref = xref.where(xref['primary_customer_flag']=='Y')

        customer_new = customer_new.join(xref, on='customer_key', how='left')

        *hidden* = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{future}/*.parquet').select('account_key','average_collected_balance_mtd')
        customer_new = customer_new.join(*hidden*, on='account_key', how='left')

        customer_new = customer_new.groupBy('utc_timestamp').agg(sum('average_collected_balance_mtd').alias('deposit_dollars_new'))
           
        customer_new.createOrReplaceTempView('temp_df')
        
        spark.sql("""
        merge into power_bi_data_products.new_customer_dollars sink
        using temp_df source
        on source.utc_timestamp = sink.utc_timestamp
        when matched then update set *
        when not matched then insert *""")
       
        
    else:
        continue
        

# COMMAND ----------


