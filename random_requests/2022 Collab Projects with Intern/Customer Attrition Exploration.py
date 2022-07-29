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

date_df= spark.table('power_bi_data_products.retail_new_customers_3')

date_df = date_df.select('utc_timestamp').dropDuplicates()

date_df = date_df.orderBy('utc_timestamp')
date_df = date_df.where(date_df.utc_timestamp >= datetime.date(2021,8,1))
date_list = date_df.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

# let me show you how to do this loop:
result = []

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
    
        #print(processing_date)
        #print(date_list_filtered)    
        #print(future_date)
        #print("-----")
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

        dd_master = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{future}/*.parquet').select('account_key','average_collected_balance_mtd')
        customer_new = customer_new.join(dd_master, on='account_key', how='left')

        customer_new = customer_new.groupBy('utc_timestamp').agg(sum('average_collected_balance_mtd').alias('new_customer_me_average_balance'))
           
        #customer_new.createOrReplaceTempView('temp_df')
        
        result.append(customer_new.rdd.flatMap(lambda x: x).collect())
        
    else:
        # no dates in date_list_filtered == move along mr. loop
        continue
        
# # define the schema of our dataframe we want to create from the result list of lists
schema = StructType([StructField("utc_timestamp", DateType(), True), 
                      StructField("new_customer_me_average_balance", DecimalType(), True)])

# # create the dataframe result_df from result (list of lists from loop output) and our defined schema
result_df = spark.createDataFrame(result, schema)

# # view that bugger
display(result_df)

# ooo fancy... Thank you!

# COMMAND ----------


