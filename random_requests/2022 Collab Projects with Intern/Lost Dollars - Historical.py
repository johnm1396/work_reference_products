# Databricks notebook source
#DEPENDENCY: RETAIL NEW CUSTOMERS 3 MUST BE POPULATED FIRST

# fix naming conventions for prod please. Getting better though for sure!

# COMMAND ----------

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
# MAGIC CREATE TABLE IF NOT EXISTS power_bi_data_products.lost_customer_dollars (utc_timestamp DATE, deposit_dollars_lost BIGINT)
# MAGIC USING DELTA

# COMMAND ----------

# get date list
date_df = spark.table('power_bi_data_products.retail_new_customers_3')
date_df = date_df.select('utc_timestamp').dropDuplicates()
date_df = date_df.orderBy('utc_timestamp')
date_df = date_df.where((date_df.utc_timestamp >= datetime.date(2021,2,1)))
date_list = date_df.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

# start main loop
for item in date_list:
    
    # define empty list to append desired file dates to
    date_list_filtered = []
    
    # define two months ago for the item being processed
    two_months_ago = item + relativedelta(months=-2)
    
    # start sub-loop to generate filtered date list
    for date in date_list:
        # append all dates from date_list to date_list_filtered that are less than or equal to two months ago 
        if date <= two_months_ago:
            date_list_filtered.append(date)
    
    # if list to check if date_list_filtered has data, if so we want to do stuff 
    if len(date_list_filtered) > 0:
        # define current as the current date being processed
        current = item
        # define prior as the last item in our date_list_filtered which will correspond to the 
        # closest file to two months prior to the date being processed
        prior = date_list_filtered[-1]
        
        # define filepath
        year = prior.strftime("%Y")
        month = prior.strftime("%m")
        day = prior.strftime("%d")
        prior_file = "".join(str(x) for x in year + "/" + month + "/" + day)
        
    
        customer = spark.table('power_bi_data_products.retail_new_customers_3')
        # filter to records that identify a lost customer
        customer_lost = customer.where(customer.lost_customer_flag == 1)
        # filter to records that were on the date being processed
        customer_lost = customer_lost.where(customer_lost.utc_timestamp == lit(current))
        
        # read in the xref file from the prior date found above using prior_file
        # select only customer_key, account_key, and primary_customer_flag
        xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{prior_file}/*.parquet").select('customer_key','account_key','primary_customer_flag')
        # filter to records that represent primary ownership only
        xref = xref.where(xref['primary_customer_flag']=='Y')
        
        # join xref to our lost customers defined above in customer_lost df
        customer_lost = customer_lost.join(xref, on='customer_key', how='left')
        
        # read in dd_master file from the prior date found above using prior_file
        dd_master = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{prior_file}/*.parquet').select('account_key','average_collected_balance_last_12_months')
        
        # join dd_master to our customer_lost df
        customer_lost = customer_lost.join(dd_master, on='account_key', how='left')
        
       
        customer_lost = customer_lost.groupBy('utc_timestamp').agg(sum('average_collected_balance_last_12_months').alias('deposit_dollars_lost'))
        
        customer_lost.createOrReplaceTempView('temp_df')
        
        spark.sql("""
        merge into power_bi_data_products.lost_customer_dollars sink
        using temp_df source
        on source.utc_timestamp = sink.utc_timestamp
        when matched then update set *
        when not matched then insert *""")
       
        
    else:
        continue
        


# COMMAND ----------


