# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.types import IntegerType, DateType

# COMMAND ----------

# date variables
todays_date = date.today()

# date constructs for filepaths
year = todays_date.strftime("%Y")
month = todays_date.strftime("%m")
day = todays_date.strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

# this table is going to be xref based but joining dimensional data into it to create OBT... so...

# read in *hidden*
xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")

# read in *hidden*, rename top to product_group, select app_code and product_group, drop duplicates and
# join product_group to xref to get product_group
product_group = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").withColumnRenamed('top', 'product_group').select('application_code', 'product_group').dropDuplicates()

xref = xref.join(product_group, xref.application == product_group.application_code, 'left')

# read in *hidden*, select relationship_key and ownership_type, drop duplicates, join rmrel to xref to get ownership_type
rmrel = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('relationship_key', 'ownership_type').dropDuplicates()

xref = xref.join(rmrel, 'relationship_key', 'left')

# read in *hidden*, we are ONLY using this for conformed status... this is NOT the best place to add context via 
# additional dimensional data - the individual packages themselves should be used for that
all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
all_prod = all_prod.select('account_key', 'date_key', 'product_key', 'status_key')

# read in *hidden* and do a bunch of stuff to get conformed_status
sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.where((sicod.code_type == 'STAT') | (sicod.code_type == 'SS') | (sicod.code_type == 'LNST') | (sicod.code_type == 'CLSD'))
sicod = sicod.where(sicod.application != 'SI')
sicod = sicod.withColumn('conformed_status', when(col('code_description_2') == '', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
sicod = sicod.select('code_key', 'conformed_status')
sicod = sicod.dropDuplicates()
all_prod = all_prod.join(sicod, all_prod.status_key == sicod.code_key, 'left').select('account_key', 'conformed_status').fillna('OPEN')

xref = xref.join(all_prod, 'account_key', 'left').fillna('PURGED', subset=['conformed_status'])

# COMMAND ----------

xref = xref.where(xref.product_group.isin('Deposit', 'Lending', 'Services'))
xref = xref.where(xref.ownership_type == 'Direct')
xref = xref.where(xref.conformed_status.isin('OPEN', 'DORMANT'))

# COMMAND ----------

xref = xref.withColumn('customer_key', xref.customer_key.cast(IntegerType()))

# COMMAND ----------

cust_type = spark.read.option("header", True).option("inferSchema", True).csv(f"/mnt/*hidden*")

# COMMAND ----------

display(cust_type)

# COMMAND ----------

xref = xref.join(cust_type, xref.customer_key == cust_type['Customer Key'], how='left')

# COMMAND ----------

xref = xref.where(xref['Customer Type']=='Personal')

# COMMAND ----------

xref = xref.dropDuplicates(subset=['customer_key', 'account_key'])

# COMMAND ----------

display(xref)

# COMMAND ----------

top_100_cust = xref.groupBy('customer_key').agg(countDistinct('account_key').alias('num_prod_svcs')).orderBy('num_prod_svcs', ascending=False)

# COMMAND ----------

dynamic_branch = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('customer_key', 'dynamic_branch_assignment', 'is_active', 'is_direct')

top_100_cust = top_100_cust.join(dynamic_branch, on='customer_key', how='left')

# COMMAND ----------

display(top_100_cust)

# COMMAND ----------


