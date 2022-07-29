# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dateutil.relativedelta import relativedelta
import datetime
import calendar

# COMMAND ----------

date_df= spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')

date_df = date_df.select('utc_timestamp').dropDuplicates()

date_df = date_df.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

date_df = date_df.orderBy('utc_timestamp')

date_list = date_df.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

current_date_list = []

for item in date_list:
    today = datetime.date.today()
    prior_mo = today + relativedelta(months=-1)
    prior_me = prior_mo.replace(day = calendar.monthrange(prior_mo.year, prior_mo.month)[1])
    
    if item <= prior_me:
        current_date_list.append(item)
        
current = current_date_list[-1]


two_months_before = current + relativedelta(months=-2)
mb_two_months_before = two_months_before.replace(day = 1)
me_two_months_before = two_months_before.replace(day = calendar.monthrange(two_months_before.year, two_months_before.month)[1])


date_list_filtered = []

for item in date_list:
    if item >= mb_two_months_before and item <= me_two_months_before:
        date_list_filtered.append(item)


for item in date_list_filtered:         
    processing_date = item
    future_date = current   

    year = future_date.strftime("%Y")
    month = future_date.strftime("%m")
    day = future_date.strftime("%d")
    future_file = "".join(str(x) for x in year + "/" + month + "/" + day)

    customer = spark.table('power_bi_data_products.retail_new_customers_3')
    customer_new = customer.where(customer['new_customer_flag']==1)
    customer_new = customer_new.where(customer_new['utc_timestamp']==lit(processing_date))

    xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{future_file}/*.parquet").select('customer_key','account_key', 'relationship_key','primary_customer_flag')
    xref = xref.where(xref['primary_customer_flag']=='Y')

    customer_new = customer_new.join(xref, on='customer_key', how='left')

    *hidden* = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{future_file}/*.parquet').select('account_key','average_collected_balance_mtd')
    customer_new = customer_new.join(*hidden*, on='account_key', how='left')

    customer_new = customer_new.groupBy('utc_timestamp').agg(sum('average_collected_balance_mtd').alias('deposit_dollars_new'))
    

    
#     customer_new.createOrReplaceTempView('temp_df')

#     spark.sql("""
#         merge into power_bi_data_products.new_customer_dollars sink
#         using temp_df source
#         on source.utc_timestamp = sink.utc_timestamp
#         when matched then update set *
#         when not matched then insert *""")
       


        

# COMMAND ----------


