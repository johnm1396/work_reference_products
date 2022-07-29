# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, year, quarter
from pyspark.sql.types import IntegerType, DateType

df = spark.createDataFrame([(date.today() + relativedelta(months=-1), 1)], ['date', 'row'])
df = df.withColumn('prior_me', last_day(df.date))
prior_me = df.agg({'prior_me':'max'}).rdd.map(lambda x: x[0]).collect()[0]

year = date.today().strftime("%Y")
month = date.today().strftime("%m")
day = date.today().strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day + "/" + "*.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE DATABASE AND TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS products_per_customer
# MAGIC LOCATION '/mnt/*hidden*/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS products_per_customer.dynamic_assignments_me (customer_key INT, me_utc_timestamp DATE, dynamic_branch_name STRING, dynamic_officer_assignment STRING)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS products_per_customer.xref_usable (account_key STRING, customer_key INT, primary_customer_flag STRING, ownership_type STRING, conformed_status STRING, retail_product_designation STRING, me_utc_timestamp DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC HISTORICAL MONTH END DYNAMIC ASSIGNMENTS - DELTA TABLE POPULATION

# COMMAND ----------

# create Month End Dynamic Assignments delta table
dynamic_assignments = spark.table('dynamic_branch_assignments.dynamic_branch_assignments')

dynamic_assignments = dynamic_assignments.where((dynamic_assignments.utc_timestamp >= date(2021, 1, 1)) & (dynamic_assignments.utc_timestamp <= prior_me))
dynamic_assignments = dynamic_assignments.where((dynamic_assignments.primary_branch_name.isNotNull()) | (dynamic_assignments.direct_branch_name.isNotNull()))
dynamic_assignments = dynamic_assignments.where(dynamic_assignments.is_direct == 'Y')
dynamic_assignments = dynamic_assignments.select('customer_key', 'dynamic_branch_name', 'utc_timestamp')

rm_mast = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")
rm_mast = rm_mast.select('customer_key', 'primary_officer', 'utc_timestamp')
rm_mast = rm_mast.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))
rm_mast = rm_mast.withColumn('customer_key', rm_mast.customer_key.cast(IntegerType()))
rm_mast = rm_mast.dropDuplicates()

dynamic_assignments = dynamic_assignments.join(rm_mast, ((dynamic_assignments.customer_key == rm_mast.customer_key) & (dynamic_assignments.utc_timestamp == rm_mast.utc_timestamp)), 'left').select(dynamic_assignments.customer_key, dynamic_assignments.dynamic_branch_name, rm_mast.primary_officer, dynamic_assignments.utc_timestamp).orderBy(['customer_key', 'utc_timestamp'])

dynamic_assignments = dynamic_assignments.withColumn('me_utc_timestamp', last_day(dynamic_assignments.utc_timestamp))

dynamic_assignments = dynamic_assignments.select('customer_key', 'dynamic_branch_name', 'primary_officer', 'me_utc_timestamp')

# COMMAND ----------

# create Monthly Dynamic Branch Assignments
monthly_dynamic_assignments_branch = dynamic_assignments.groupBy(['customer_key', 'me_utc_timestamp', 'dynamic_branch_name']).count().orderBy(['customer_key', 'me_utc_timestamp', 'dynamic_branch_name'])
window = Window.partitionBy(['customer_key', 'me_utc_timestamp']).orderBy(col('count').desc())
monthly_dynamic_assignments_branch = monthly_dynamic_assignments_branch.withColumn('row', row_number().over(window)).filter(col('row') == 1).drop('row').drop('count').orderBy(['customer_key', 'me_utc_timestamp'])

# COMMAND ----------

monthly_dynamic_assignments_officer = dynamic_assignments.groupBy(['customer_key', 'me_utc_timestamp', 'primary_officer']).count().orderBy(['customer_key', 'me_utc_timestamp', 'primary_officer'])
window = Window.partitionBy(['customer_key', 'me_utc_timestamp']).orderBy(col('count').desc())
monthly_dynamic_assignments_officer = monthly_dynamic_assignments_officer.withColumn('row', row_number().over(window)).filter(col('row') == 1).drop('row').drop('count').orderBy(['customer_key', 'me_utc_timestamp'])
monthly_dynamic_assignments_officer = monthly_dynamic_assignments_officer.withColumnRenamed('customer_key', 'customer_key_off')
monthly_dynamic_assignments_officer = monthly_dynamic_assignments_officer.withColumnRenamed('me_utc_timestamp', 'me_utc_timestamp_off')

# COMMAND ----------

# create Month End Dynamic Assignments delta table
monthly_dynamic_assignments = monthly_dynamic_assignments_branch.join(monthly_dynamic_assignments_officer, ((monthly_dynamic_assignments_branch.customer_key == monthly_dynamic_assignments_officer.customer_key_off) & (monthly_dynamic_assignments_branch.me_utc_timestamp == monthly_dynamic_assignments_officer.me_utc_timestamp_off)), 'inner')
monthly_dynamic_assignments = monthly_dynamic_assignments.select('customer_key', 'me_utc_timestamp', 'dynamic_branch_name', 'primary_officer')
monthly_dynamic_assignments = monthly_dynamic_assignments.withColumnRenamed('primary_officer', 'dynamic_officer_assignment')

monthly_dynamic_assignments.write.mode("overwrite").saveAsTable('products_per_customer.dynamic_assignments_me')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC HISTORICAL MONTH END XREF - DELTA TABLE POPULATION

# COMMAND ----------

# BUILD A USABLE XREF
# Step 1: get xref as of ME for all months of interest 
# Step 2: join xref with rmrel on relationship_key & me_utc_timestamp to get ownership_type at ME
# Step 3: get all_prod as of ME for all months of interest
# Step 4: join all_prod with *hidden* on code_key(status_key) & me_utc_timestamp to get conformed_status
# Step 5: join xref with all_prod on account_key & me_utc_timestamp to get product_key and conformed_status
# Step 6: join xref with curated_db.product_criteria to get retail_product_designation
# resulting xref_usable delta table = account_key, customer_key, primary_customer_flag, ownership_type, conformed_status,
#                                     retail_product_designation, and me_utc_timestamp at ME for all months of interest

# read in *hidden*
xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/*/*/*/*.parquet")

xref = xref.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

xref = xref.where((xref.utc_timestamp == date(2021, 1, 29)) | (xref.utc_timestamp == date(2021, 2, 26)) | (xref.utc_timestamp == date(2021, 3, 31)) | (xref.utc_timestamp == date(2021, 4, 30)) | (xref.utc_timestamp == date(2021, 5, 28)) | (xref.utc_timestamp == date(2021, 6, 30)) | (xref.utc_timestamp == date(2021, 7, 30)) | (xref.utc_timestamp == date(2021, 8, 31)) | (xref.utc_timestamp == date(2021, 9, 30)) | (xref.utc_timestamp == date(2021, 10, 29)) | (xref.utc_timestamp == date(2021, 11, 30)) | (xref.utc_timestamp == date(2021, 12, 31)) | (xref.utc_timestamp == date(2022, 1, 31)) | (xref.utc_timestamp == date(2022, 2, 28)) | (xref.utc_timestamp == date(2022, 3, 31)))

xref = xref.withColumn('me_utc_timestamp', last_day(xref.utc_timestamp))
xref = xref.drop('utc_timestamp')
xref = xref.select('account_key', 'customer_key', 'primary_customer_flag', 'relationship_key', 'me_utc_timestamp')

# COMMAND ----------

# read in fi_core_relationship, select relationship_key and ownership_type, drop duplicates, join rmrel to xref to get ownership_type
rmrel = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')

rmrel = rmrel.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

rmrel = rmrel.where((rmrel.utc_timestamp == date(2021, 1, 29)) | (rmrel.utc_timestamp == date(2021, 2, 26)) | (rmrel.utc_timestamp == date(2021, 3, 31)) | (rmrel.utc_timestamp == date(2021, 4, 30)) | (rmrel.utc_timestamp == date(2021, 5, 28)) | (rmrel.utc_timestamp == date(2021, 6, 30)) | (rmrel.utc_timestamp == date(2021, 7, 30)) | (rmrel.utc_timestamp == date(2021, 8, 31)) | (rmrel.utc_timestamp == date(2021, 9, 30)) | (rmrel.utc_timestamp == date(2021, 10, 29)) | (rmrel.utc_timestamp == date(2021, 11, 30)) | (rmrel.utc_timestamp == date(2021, 12, 31)) | (rmrel.utc_timestamp == date(2022, 1, 31)) | (rmrel.utc_timestamp == date(2022, 2, 28)) | (rmrel.utc_timestamp == date(2022, 3, 31)))

rmrel = rmrel.withColumn('me_utc_timestamp', last_day(rmrel.utc_timestamp))
rmrel = rmrel.drop('utc_timestamp')

rmrel = rmrel.select('relationship_key', 'ownership_type', 'me_utc_timestamp').dropDuplicates()

# COMMAND ----------

xref = xref.join(rmrel, ((xref.relationship_key == rmrel.relationship_key) & (xref.me_utc_timestamp == rmrel.me_utc_timestamp)), 'left').select(xref.account_key, xref.customer_key, xref.primary_customer_flag, rmrel.ownership_type, xref.me_utc_timestamp)

# COMMAND ----------

# read in allprod, select acct_key date_key product_key and status_key
all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')
all_prod = all_prod.select('account_key', 'product_key', 'status_key', 'utc_timestamp')

all_prod = all_prod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

all_prod = all_prod.where((all_prod.utc_timestamp == date(2021, 1, 29)) | (all_prod.utc_timestamp == date(2021, 2, 26)) | (all_prod.utc_timestamp == date(2021, 3, 31)) | (all_prod.utc_timestamp == date(2021, 4, 30)) | (all_prod.utc_timestamp == date(2021, 5, 28)) | (all_prod.utc_timestamp == date(2021, 6, 30)) | (all_prod.utc_timestamp == date(2021, 7, 30)) | (all_prod.utc_timestamp == date(2021, 8, 31)) | (all_prod.utc_timestamp == date(2021, 9, 30)) | (all_prod.utc_timestamp == date(2021, 10, 29)) | (all_prod.utc_timestamp == date(2021, 11, 30)) | (all_prod.utc_timestamp == date(2021, 12, 31)) | (all_prod.utc_timestamp == date(2022, 1, 31)) | (all_prod.utc_timestamp == date(2022, 2, 28)) | (all_prod.utc_timestamp == date(2022, 3, 31)))

all_prod = all_prod.withColumn('me_utc_timestamp', last_day(all_prod.utc_timestamp))
all_prod = all_prod.drop('utc_timestamp')

# COMMAND ----------

# read in *hidden* and do a bunch of stuff to get conformed_status
sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')

sicod = sicod.withColumn('utc_timestamp', expr("""case when cast(left(utc_timestamp,10) as date) is null then to_date(left(utc_timestamp,10),'M/dd/yyyy') else cast(left(utc_timestamp,10) as date) end"""))

sicod = sicod.where((sicod.utc_timestamp == date(2021, 1, 29)) | (sicod.utc_timestamp == date(2021, 2, 26)) | (sicod.utc_timestamp == date(2021, 3, 31)) | (sicod.utc_timestamp == date(2021, 4, 30)) | (sicod.utc_timestamp == date(2021, 5, 28)) | (sicod.utc_timestamp == date(2021, 6, 30)) | (sicod.utc_timestamp == date(2021, 7, 30)) | (sicod.utc_timestamp == date(2021, 8, 31)) | (sicod.utc_timestamp == date(2021, 9, 30)) | (sicod.utc_timestamp == date(2021, 10, 29)) | (sicod.utc_timestamp == date(2021, 11, 30)) | (sicod.utc_timestamp == date(2021, 12, 31)) | (sicod.utc_timestamp == date(2022, 1, 31)) | (sicod.utc_timestamp == date(2022, 2, 28)) | (sicod.utc_timestamp == date(2022, 3, 31)))

sicod = sicod.withColumn('me_utc_timestamp', last_day(sicod.utc_timestamp))
sicod = sicod.drop('utc_timestamp')

sicod = sicod.where((sicod.code_type == 'STAT') | (sicod.code_type == 'SS') | (sicod.code_type == 'LNST') | (sicod.code_type == 'CLSD'))
sicod = sicod.where(sicod.application != 'SI')
sicod = sicod.withColumn('conformed_status', when(col('code_description_2') == '', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))

sicod = sicod.select('code_key', 'me_utc_timestamp', 'conformed_status').dropDuplicates()

# COMMAND ----------

all_prod = all_prod.join(sicod, ((all_prod.status_key == sicod.code_key) & (all_prod.me_utc_timestamp == sicod.me_utc_timestamp)), 'left').select(all_prod.account_key, all_prod.product_key, sicod.conformed_status, all_prod.me_utc_timestamp).dropDuplicates()

# COMMAND ----------

xref = xref.join(all_prod, ((xref.account_key == all_prod.account_key) & (xref.me_utc_timestamp == all_prod.me_utc_timestamp)), 'left').select(xref.account_key, xref.customer_key, xref.primary_customer_flag, xref.ownership_type, all_prod.conformed_status, all_prod.product_key, xref.me_utc_timestamp).fillna('OPEN', subset=['conformed_status'])

# COMMAND ----------

product_lookup = spark.table('curated_db.product_criteria') 
product_lookup = product_lookup.where((product_lookup.product_key != '1-03-') & (product_lookup.product_key != '1-04-') & (product_lookup.product_key != '1-05-') & (product_lookup.product_key != '1-51-') & (product_lookup.product_key != '1-56-') & (product_lookup.product_key != '1-57-')) # & (product_lookup.product_key != '1-69-') & (product_lookup.product_key != '1-70-')) #I am leaving these in even though we know the service doesn't work correctly as they are important enough that they need to be accounted for
product_lookup = product_lookup.select('product_key', 'retail_product_designation').dropDuplicates()
product_lookup = product_lookup.withColumnRenamed('product_key', 'product_key_lkp')

# COMMAND ----------

xref = xref.join(product_lookup, xref.product_key == product_lookup.product_key_lkp, 'left').select('account_key', 'customer_key', 'primary_customer_flag', 'ownership_type', 'conformed_status', 'retail_product_designation', 'me_utc_timestamp')

# COMMAND ----------

xref = xref.where(xref.retail_product_designation.isin('Product', 'Service'))

# COMMAND ----------

xref = xref.withColumn('customer_key', xref.customer_key.cast(IntegerType()))

# COMMAND ----------

xref.write.mode("overwrite").saveAsTable('products_per_customer.xref_usable')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC CREATE PRODUCT_COUNTS FROM THE TWO DELTA TABLES ABOVE

# COMMAND ----------

# read in delta tables from above

# remove closed branches
monthly_dynamic_assignments = spark.table('products_per_customer.dynamic_assignments_me')
monthly_dynamic_assignments = monthly_dynamic_assignments.where((monthly_dynamic_assignments.dynamic_branch_name != '*hidden*') & (monthly_dynamic_assignments.dynamic_branch_name != '*hidden*') & (monthly_dynamic_assignments.dynamic_branch_name != '*hidden*'))

xref_usable = spark.table('products_per_customer.xref_usable')

# create product_counts using xref_usable and monthly_dynamic_assignments delta tables
product_counts = xref_usable.join(monthly_dynamic_assignments, ((xref_usable.customer_key == monthly_dynamic_assignments.customer_key) & (xref_usable.me_utc_timestamp == monthly_dynamic_assignments.me_utc_timestamp)), 'left').select(xref_usable.account_key, xref_usable.customer_key, xref_usable.primary_customer_flag, xref_usable.ownership_type, xref_usable.conformed_status, xref_usable.retail_product_designation, monthly_dynamic_assignments.dynamic_branch_name, monthly_dynamic_assignments.dynamic_officer_assignment, xref_usable.me_utc_timestamp)

# filter to direct, rtl, and open/dormant
product_counts = product_counts.where((product_counts.ownership_type == 'Direct') & (product_counts.dynamic_officer_assignment == 'RTL'))
product_counts = product_counts.where((product_counts.conformed_status=='OPEN')|(product_counts.conformed_status=='DORMANT'))

# create quarter
product_counts = product_counts.withColumn('quarter', date_format(product_counts.me_utc_timestamp, "yyyy_Q"))

# select only needed columns
product_counts = product_counts.select('dynamic_branch_name', 'customer_key', 'me_utc_timestamp', 'quarter', 'account_key')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC GET PRIOR YEAR QUARTERLY AVGs

# COMMAND ----------

# filter product_counts to prior year
prior_year_Q_avgs = product_counts.where(date_format(product_counts.me_utc_timestamp, "yyyy") == (date.today() - timedelta(365)).strftime("%Y"))

# create a list from quarters
prior_year_Q_list = prior_year_Q_avgs.select('quarter').withColumn('quarter', prior_year_Q_avgs.quarter.cast('string'))
prior_year_Q_list = prior_year_Q_list.dropDuplicates()
prior_year_Q_list = prior_year_Q_list.rdd.map(lambda x: x[0]).collect()
prior_year_Q_list.sort()

# generate dict for replace
if len(prior_year_Q_list) == 4:
  prior_year_Q_dict = {prior_year_Q_list[0]:'prior_year_Q1', prior_year_Q_list[1]:'prior_year_Q2', prior_year_Q_list[2]:'prior_year_Q3', prior_year_Q_list[3]:'prior_year_Q4'}
elif len(prior_year_Q_list) == 3:
  prior_year_Q_dict = {prior_year_Q_list[0]:'prior_year_Q1', prior_year_Q_list[1]:'prior_year_Q2', prior_year_Q_list[2]:'prior_year_Q3'}
elif len(prior_year_Q_list) == 2:
  prior_year_Q_dict = {prior_year_Q_list[0]:'prior_year_Q1', prior_year_Q_list[1]:'prior_year_Q2'}
elif len(prior_year_Q_list) == 1:
  prior_year_Q_dict = {prior_year_Q_list[0]:'prior_year_Q1'}
else:
  prior_year_Q_dict = {}
  
# process averages by quarter for prior year
prior_year_Q_avgs = prior_year_Q_avgs.na.replace(prior_year_Q_dict, subset=['quarter'])

prior_year_Q_avgs = prior_year_Q_avgs.groupBy(['dynamic_branch_name', 'customer_key', 'me_utc_timestamp', 'quarter']).agg(countDistinct('account_key').alias('num_prod_svc'))

prior_year_Q_avgs = prior_year_Q_avgs.select('dynamic_branch_name', 'me_utc_timestamp', 'quarter', 'num_prod_svc')

prior_year_Q_avgs = prior_year_Q_avgs.groupBy(['dynamic_branch_name', 'me_utc_timestamp', 'quarter']).agg(avg('num_prod_svc').alias('avg_num_prod_svc'))

prior_year_Q_avgs = prior_year_Q_avgs.groupBy(['dynamic_branch_name', 'quarter']).agg(avg('avg_num_prod_svc').alias('avg_num_prod_svc'))

prior_year_Q_avgs = prior_year_Q_avgs.groupBy("dynamic_branch_name").pivot("quarter").max("avg_num_prod_svc")

# COMMAND ----------

display(prior_year_Q_avgs)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC GET CURRENT YEAR GOAL

# COMMAND ----------

# pivot product_counts to get current_year goal (prior year annual stddev)
prior_year_stddev = product_counts.where(date_format(product_counts.me_utc_timestamp, "yyyy") == (date.today() - timedelta(365)).strftime("%Y"))

prior_year_stddev = prior_year_stddev.select('dynamic_branch_name', 'customer_key', 'me_utc_timestamp', 'account_key')

prior_year_stddev = prior_year_stddev.groupBy(['dynamic_branch_name', 'customer_key', 'me_utc_timestamp']).agg(countDistinct('account_key').alias('num_prod_svc'))

prior_year_stddev = prior_year_stddev.select('dynamic_branch_name', 'me_utc_timestamp', 'num_prod_svc')

prior_year_stddev = prior_year_stddev.groupBy(['dynamic_branch_name', 'me_utc_timestamp']).agg(avg('num_prod_svc').alias('avg_num_prod_svc'))

prior_year_stddev = prior_year_stddev.select('dynamic_branch_name', 'avg_num_prod_svc')

prior_year_stddev = prior_year_stddev.groupBy("dynamic_branch_name").agg(stddev('avg_num_prod_svc').alias('current_year_annual_goal'))

# COMMAND ----------

display(prior_year_stddev)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC GET CURRENT YEAR QUARTERLY AVGs

# COMMAND ----------

# filter product_counts to prior year
current_year_Q_avgs = product_counts.where(date_format(product_counts.me_utc_timestamp, "yyyy") == (date.today()).strftime("%Y"))

# create a list from quarters
current_year_Q_list = current_year_Q_avgs.select('quarter').withColumn('quarter', current_year_Q_avgs.quarter.cast('string'))
current_year_Q_list = current_year_Q_list.dropDuplicates()
current_year_Q_list = current_year_Q_list.rdd.map(lambda x: x[0]).collect()
current_year_Q_list.sort()

# generate dict for replace
if len(current_year_Q_list) == 4:
  current_year_Q_dict = {current_year_Q_list[0]:'current_year_Q1', current_year_Q_list[1]:'current_year_Q2', current_year_Q_list[2]:'current_year_Q3', current_year_Q_list[3]:'current_year_Q4'}
elif len(current_year_Q_list) == 3:
  current_year_Q_dict = {current_year_Q_list[0]:'current_year_Q1', current_year_Q_list[1]:'current_year_Q2', current_year_Q_list[2]:'current_year_Q3'}
elif len(current_year_Q_list) == 2:
  current_year_Q_dict = {current_year_Q_list[0]:'current_year_Q1', current_year_Q_list[1]:'current_year_Q2'}
elif len(current_year_Q_list) == 1:
  current_year_Q_dict = {current_year_Q_list[0]:'current_year_Q1'}
else:
  current_year_Q_dict = {}
  
# process averages by quarter for prior year
current_year_Q_avgs = current_year_Q_avgs.na.replace(current_year_Q_dict, subset=['quarter'])

current_year_Q_avgs = current_year_Q_avgs.groupBy(['dynamic_branch_name', 'customer_key', 'me_utc_timestamp', 'quarter']).agg(countDistinct('account_key').alias('num_prod_svc'))

current_year_Q_avgs = current_year_Q_avgs.select('dynamic_branch_name', 'me_utc_timestamp', 'quarter', 'num_prod_svc')

current_year_Q_avgs = current_year_Q_avgs.groupBy(['dynamic_branch_name', 'me_utc_timestamp', 'quarter']).agg(avg('num_prod_svc').alias('avg_num_prod_svc'))

current_year_Q_avgs = current_year_Q_avgs.groupBy(['dynamic_branch_name', 'quarter']).agg(avg('avg_num_prod_svc').alias('avg_num_prod_svc'))

current_year_Q_avgs = current_year_Q_avgs.groupBy("dynamic_branch_name").pivot("quarter").max("avg_num_prod_svc")

# COMMAND ----------

display(current_year_Q_avgs)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC GET PRIOR YEAR END NUM CUST AND NUM PROD FOR ESTIMATED GOALS ---- I DO NOT THINK THIS LOGICALLY MAKES SENSE FOR EST GOALS BUT IT's NOT MY CALL... ZZZ

# COMMAND ----------

# # filter product_counts to prior year
# prior_year_end_num = product_counts.where(date_format(product_counts.me_utc_timestamp, "yyyy") == (date.today() - timedelta(365)).strftime("%Y"))

# max_date = prior_year_end_num.select('me_utc_timestamp').rdd.max()[0]

# prior_year_end_num = prior_year_end_num.where(prior_year_end_num.me_utc_timestamp == max_date)

# prior_year_end_num = prior_year_end_num.groupBy(['dynamic_branch_name']).agg(countDistinct('customer_key').alias('prior_ye_num_cust'), countDistinct('account_key').alias('prior_ye_num_prod_svc'))

# COMMAND ----------

# display(prior_year_end_num)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC BUILD USABLE DF FOR CALCS

# COMMAND ----------

branch_pspc = prior_year_Q_avgs
branch_pspc = branch_pspc.join(prior_year_stddev, on='dynamic_branch_name', how='left')
branch_pspc = branch_pspc.join(prior_year_end_num, on='dynamic_branch_name', how='left')
branch_pspc = branch_pspc.join(current_year_Q_avgs, on='dynamic_branch_name', how='left')

# add q1 goals
branch_pspc = branch_pspc.withColumn('current_year_q1_1pt_goal', branch_pspc.prior_year_Q4)
branch_pspc = branch_pspc.withColumn('current_year_q1_3pt_goal', (branch_pspc.prior_year_Q4 + ((branch_pspc.current_year_annual_goal / 4)*1)))
branch_pspc = branch_pspc.withColumn('current_year_q1_2pt_goal', ((branch_pspc.current_year_q1_1pt_goal + branch_pspc.current_year_q1_3pt_goal) / 2))
branch_pspc = branch_pspc.withColumn('current_year_q1_4pt_goal', (branch_pspc.current_year_q1_3pt_goal + (branch_pspc.current_year_q1_2pt_goal - branch_pspc.current_year_q1_1pt_goal)))
branch_pspc = branch_pspc.withColumn('current_year_q1_5pt_goal', (branch_pspc.current_year_q1_4pt_goal + (branch_pspc.current_year_q1_2pt_goal - branch_pspc.current_year_q1_1pt_goal)))

# add q2 goals
branch_pspc = branch_pspc.withColumn('current_year_q2_1pt_goal', branch_pspc.current_year_q1_3pt_goal)
branch_pspc = branch_pspc.withColumn('current_year_q2_3pt_goal', (branch_pspc.prior_year_Q4 + ((branch_pspc.current_year_annual_goal / 4)*2)))
branch_pspc = branch_pspc.withColumn('current_year_q2_2pt_goal', ((branch_pspc.current_year_q2_1pt_goal + branch_pspc.current_year_q2_3pt_goal) / 2))
branch_pspc = branch_pspc.withColumn('current_year_q2_4pt_goal', (branch_pspc.current_year_q2_3pt_goal + (branch_pspc.current_year_q2_2pt_goal - branch_pspc.current_year_q2_1pt_goal)))
branch_pspc = branch_pspc.withColumn('current_year_q2_5pt_goal', (branch_pspc.current_year_q2_4pt_goal + (branch_pspc.current_year_q2_2pt_goal - branch_pspc.current_year_q2_1pt_goal)))

# add q3 goals
branch_pspc = branch_pspc.withColumn('current_year_q3_1pt_goal', branch_pspc.current_year_q2_3pt_goal)
branch_pspc = branch_pspc.withColumn('current_year_q3_3pt_goal', (branch_pspc.prior_year_Q4 + ((branch_pspc.current_year_annual_goal / 4)*3)))
branch_pspc = branch_pspc.withColumn('current_year_q3_2pt_goal', ((branch_pspc.current_year_q3_1pt_goal + branch_pspc.current_year_q3_3pt_goal) / 2))
branch_pspc = branch_pspc.withColumn('current_year_q3_4pt_goal', (branch_pspc.current_year_q3_3pt_goal + (branch_pspc.current_year_q3_2pt_goal - branch_pspc.current_year_q3_1pt_goal)))
branch_pspc = branch_pspc.withColumn('current_year_q3_5pt_goal', (branch_pspc.current_year_q3_4pt_goal + (branch_pspc.current_year_q3_2pt_goal - branch_pspc.current_year_q3_1pt_goal)))

# add q4 goals
branch_pspc = branch_pspc.withColumn('current_year_q4_1pt_goal', branch_pspc.current_year_q3_3pt_goal)
branch_pspc = branch_pspc.withColumn('current_year_q4_3pt_goal', (branch_pspc.prior_year_Q4 + ((branch_pspc.current_year_annual_goal / 4)*4)))
branch_pspc = branch_pspc.withColumn('current_year_q4_2pt_goal', ((branch_pspc.current_year_q4_1pt_goal + branch_pspc.current_year_q4_3pt_goal) / 2))
branch_pspc = branch_pspc.withColumn('current_year_q4_4pt_goal', (branch_pspc.current_year_q4_3pt_goal + (branch_pspc.current_year_q4_2pt_goal - branch_pspc.current_year_q4_1pt_goal)))
branch_pspc = branch_pspc.withColumn('current_year_q4_5pt_goal', (branch_pspc.current_year_q4_4pt_goal + (branch_pspc.current_year_q4_2pt_goal - branch_pspc.current_year_q4_1pt_goal)))

# COMMAND ----------

display(branch_pspc)

# COMMAND ----------

branch_pspc.write.mode("overwrite").saveAsTable("products_per_customer.pspc_q1_2022")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC old stuff

# COMMAND ----------

# # pivot product_counts to get the avg ppc by branch
# monthly_product_counts = product_counts.select('dynamic_branch_name', 'customer_key', 'me_utc_timestamp', 'account_key')
# monthly_product_counts = monthly_product_counts.groupBy(['dynamic_branch_name', 'customer_key', 'me_utc_timestamp']).agg(countDistinct('account_key').alias('num_products'))
# monthly_product_counts = monthly_product_counts.select('dynamic_branch_name', 'me_utc_timestamp', 'num_products')
# monthly_product_counts = monthly_product_counts.groupBy(['dynamic_branch_name', 'me_utc_timestamp']).agg(avg('num_products').alias('avg_num_products'))
# monthly_product_counts = monthly_product_counts.groupBy("dynamic_branch_name").pivot("me_utc_timestamp").max("avg_num_products")

# display(monthly_product_counts)



