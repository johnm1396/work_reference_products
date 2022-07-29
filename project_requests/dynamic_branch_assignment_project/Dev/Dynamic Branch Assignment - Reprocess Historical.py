# Databricks notebook source
# MAGIC %md
# MAGIC PACKAGE IMPORTS

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC TEMP CODE TO INTEGRATE IF CUSTOMER IS ACTIVE ON GIVEN DAY
# MAGIC - will need to add this code to the historical and daily processing once I figure it out
# MAGIC   - use *hidden* is_active and is_direct logic?
# MAGIC     - is_active: customer attached to any open or dormant products in *hidden*
# MAGIC     - is_direct: customer has direct ownership over a deposit or lending account

# COMMAND ----------

# read in *hidden* to get all dates it has been processed on
date_df = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")
date_df = date_df.select('utc_timestamp').dropDuplicates()

date_df = date_df.orderBy('utc_timestamp')

date_list = date_df.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

for item in date_list:
  # date variables
  todays_date = item
  
  # date constructs for filepaths
  year = todays_date.strftime("%Y")
  month = todays_date.strftime("%m")
  day = todays_date.strftime("%d")
  today = "".join(str(x) for x in year + "/" + month + "/" + day)
  
  # get daily branch assignments
  branch_assignments = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/v2_assignments/{today}/*.parquet")
  
  # get xref
  # add product_group
  xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")
  product_group = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").dropDuplicates().withColumnRenamed('top', 'product_group').select('application_code', 'product_group')
  xref = xref.join(product_group, xref.application == product_group.application_code, 'left')
  
  # add ownership type
  rmrel = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('relationship_key', 'ownership_type').dropDuplicates()
  xref = xref.join(rmrel, 'relationship_key', 'left')
  
  # add conformed status
  all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
  all_prod = all_prod.select('account_key', 'date_key', 'product_key', 'status_key')
  sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
  sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
  sicod = sicod.where(sicod.application!='SI')
  sicod = sicod.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
  sicod = sicod.select('code_key', 'conformed_status')
  sicod = sicod.dropDuplicates()
  all_prod = all_prod.join(sicod, all_prod.status_key == sicod.code_key, 'left').select('account_key', 'conformed_status').fillna('OPEN')
  xref = xref.join(all_prod, 'account_key', 'left')
  
  # select final columns
  xref = xref.select('account_key', 'product_group', 'conformed_status', 'customer_key', 'ownership_type', 'primary_customer_flag')
  
  # cast customer_key as int to trim leading zeros
  xref = xref.withColumn('customer_key', xref.customer_key.cast(IntegerType()))
  
  # get only unique instances of direct ownership over deposit/lending product - this will ignore multiple relationships
  xref_direct = xref.where((xref.ownership_type == 'Direct') & ((xref.product_group == 'Deposit') | (xref.product_group == 'Lending'))).dropDuplicates(['account_key', 'customer_key'])
  
  # get unique relationships - this will ignore multiple relationships and ownership types
  xref_unique = xref.dropDuplicates(['account_key', 'customer_key'])
  
  # create is_active and is_direct flags
  is_active = xref_unique.select('customer_key').dropDuplicates()
  is_active = is_active.withColumn('is_active', lit('Y'))
  is_direct = xref_direct.select('customer_key').dropDuplicates()
  is_direct = is_direct.withColumn('is_direct', lit('Y'))

  # add is_active and is_direct flags to daily assignment data
  branch_assignments = branch_assignments.join(is_active, 'customer_key', 'left')
  branch_assignments = branch_assignments.join(is_direct, 'customer_key', 'left')
  branch_assignments = branch_assignments.fillna('N', subset=['is_active', 'is_direct'])
  
  # reorder columns
  branch_assignments = branch_assignments.select('customer_key', 'primary_branch_number', 'direct_branch_number', 'acct_branch_number', 'opening_branch', 'dynamic_branch_assignment', 'is_active', 'is_direct', 'utc_timestamp')
  
  
  # rewrite file
  branch_assignments.coalesce(1).write.mode("overwrite").parquet(f"/mnt/*hidden*/{today}/")

# COMMAND ----------

# MAGIC %md
# MAGIC DO I NEED TO GO BACK AND APPLY BRANCH NAMES ON ALL HISTORICAL FILES???

# COMMAND ----------

# read in rm_master to get all dates it has been processed on
date_df = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")
date_df = date_df.select('utc_timestamp').dropDuplicates()

date_df = date_df.orderBy('utc_timestamp')

date_list = date_df.rdd.map(lambda x: x[0]).collect()

# COMMAND ----------

for item in date_list:
  # date variables
  todays_date = item
  
  # date constructs for filepaths
  year = todays_date.strftime("%Y")
  month = todays_date.strftime("%m")
  day = todays_date.strftime("%d")
  today = "".join(str(x) for x in year + "/" + month + "/" + day)
  
  branch_assignments = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")
  
  branch_assignments = branch_assignments.withColumn('customer_key', branch_assignments.customer_key.cast(IntegerType()))
  branch_assignments = branch_assignments.withColumn('primary_branch_number', branch_assignments.primary_branch_number.cast('string'))
  branch_assignments = branch_assignments.withColumn('direct_branch_number', branch_assignments.direct_branch_number.cast('string'))
  branch_assignments = branch_assignments.withColumn('acct_branch_number', branch_assignments.acct_branch_number.cast('string'))
  branch_assignments = branch_assignments.withColumn('opening_branch', branch_assignments.opening_branch.cast('string'))
  branch_assignments = branch_assignments.withColumn('dynamic_branch_assignment', branch_assignments.dynamic_branch_assignment.cast('string'))
  
  # convert branch numbers to names
  branch_lookup = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").select('branch_id', 'branch_description').dropDuplicates(['branch_id'])

  branch_lookup = branch_lookup.withColumn('branch_id', branch_lookup.branch_id.cast('string'))

  dict = {row['branch_id']:row['branch_description'] for row in branch_lookup.collect()}

  branch_assignments = branch_assignments.na.replace(dict, subset=['primary_branch_number', 'direct_branch_number', 'acct_branch_number', 'opening_branch', 'dynamic_branch_assignment'])

  # rename columns
  branch_assignments = branch_assignments.withColumnRenamed('primary_branch_number', 'primary_branch')
  branch_assignments = branch_assignments.withColumnRenamed('direct_branch_number', 'direct_branch')
  branch_assignments = branch_assignments.withColumnRenamed('acct_branch_number', 'acct_branch')

  # append daily assignments to delta table
  branch_assignments.coalesce(1).write.mode("overwrite").parquet(f"/mnt/*hidden*/{today}/")
