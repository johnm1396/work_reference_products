# Databricks notebook source
# MAGIC %md
# MAGIC PACKAGE IMPORTS

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType

# date variables
todays_date = date.today()
one_year_ago = todays_date - timedelta(365)

# date constructs for filepaths
year = todays_date.strftime("%Y")
month = todays_date.strftime("%m")
day = todays_date.strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE AND APPEND/OVERWRITE DAILY DATA TO DELTA TABLES

# COMMAND ----------

# read in teller xe
teller_union = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")

# filter out irrelevant transactions and drop any duplicates for the most recent version
teller_union = teller_union.where(teller_union.primary_owner_key > 0)
teller_union = teller_union.where(teller_union.account_key_1.isNotNull())
teller_union = teller_union.orderBy('utc_timestamp', ascending=False).dropDuplicates(['transaction_id'])

# cast branch_number/branch_id as int
teller_union = teller_union.withColumn('branch_number', teller_union.branch_number.cast(IntegerType()))

# select specific columns
teller_union = teller_union.select('account_key_1', 'branch_number', 'transaction_date', 'transaction_id')

# add daily output to delta table
teller_union.createOrReplaceTempView('teller_union')

spark.sql("""
merge into dynamic_branch_assignments.teller_union sink
using teller_union source
on source.transaction_id = sink.transaction_id
when not matched then insert *""")

# COMMAND ----------

# read in account data to get opened dates
accts = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")
accts = accts.orderBy('utc_timestamp', ascending=False).dropDuplicates(['account_key'])
accts = accts.select('account_key', 'application_code', 'date_opened', 'org_key')

# branch data from fi_core_org
branch_data = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")
branch_data = branch_data.orderBy('utc_timestamp', ascending=False).select('org_key', 'branch_id', 'branch_description').dropDuplicates()

# join to get branch
accts = accts.join(branch_data, 'org_key', 'left')

# drop useless rows
accts = accts.where((accts.branch_id > 0)&(accts.date_opened.isNotNull()))

# cast timestamp to date
accts = accts.withColumn('date_opened', to_date('date_opened'))

# cast branch_number/branch_id as int
accts = accts.withColumn('branch_id', accts.branch_id.cast(IntegerType()))

# select specific columns
accts = accts.select('account_key', 'date_opened', 'branch_id')

# write to delta table for speed of use later
accts.createOrReplaceTempView('accts')

spark.sql("""
merge into dynamic_branch_assignments.accts sink
using accts source
on source.account_key = sink.account_key
when matched then update set sink.branch_id = source.branch_id
when not matched then insert *""")

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE DAILY FILE

# COMMAND ----------

# CUST_DATA

# read in rm_master, drop all columns but customer_key and opening_branch, drop duplicates, cast customer_key as int
cust_data = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")
cust_data = cust_data.select('customer_key', 'opening_branch').dropDuplicates().orderBy('customer_key')
cust_data = cust_data.withColumn('customer_key', cust_data.customer_key.cast(IntegerType()))


# XREF

# read in rmxref
xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")

# read in fi_core_product, rename top to product_group, select app_code and product_group, drop duplicates and
# join product_group to xref to get product_group
product_group = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").withColumnRenamed('top', 'product_group').select('application_code', 'product_group').dropDuplicates()

xref = xref.join(product_group, xref.application == product_group.application_code, 'left')

# read in fi_core_relationship, select relationship_key and ownership_type, drop duplicates, join rmrel to xref to get ownership_type
rmrel = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('relationship_key', 'ownership_type').dropDuplicates()

xref = xref.join(rmrel, 'relationship_key', 'left')

# read in allprod, select acct_key date_key product_key and status_key
all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
all_prod = all_prod.select('account_key', 'date_key', 'product_key', 'status_key')

# read in sicod1 and do a bunch of stuff to get conformed_status
sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
sicod = sicod.where(sicod.application!='SI')
sicod = sicod.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
sicod = sicod.select('code_key', 'conformed_status')
sicod = sicod.dropDuplicates()
all_prod = all_prod.join(sicod, all_prod.status_key == sicod.code_key, 'left').select('account_key', 'conformed_status').fillna('OPEN')

xref = xref.join(all_prod, 'account_key', 'left')

# select only needed columns
xref = xref.select('account_key', 'product_group', 'conformed_status', 'customer_key', 'ownership_type', 'primary_customer_flag')

# cast customer_key as int to trim leading zeros
xref = xref.withColumn('customer_key', xref.customer_key.cast(IntegerType()))

# get only unique instances of direct ownership for use with teller trans
xref_direct = xref.where(xref.ownership_type == 'Direct').dropDuplicates(['account_key', 'customer_key'])


# CALCULATE FIELDS FOR DYNAMIC BRANCH ASSIGNMENT LOGIC

# join xref_direct to teller_union to get direct teller trans table
direct = spark.table("dynamic_branch_assignments.teller_union")
direct = direct.join(xref_direct, direct.account_key_1 == xref_direct.account_key, 'left')
direct = direct.where(direct.ownership_type.isNotNull())

# create primary teller trans table from direct_trans
primary = direct.where(direct.primary_customer_flag == 'Y')

# join xref_direct to accts
accts = spark.table("dynamic_branch_assignments.accts")
accts = accts.join(xref_direct, 'account_key', 'left')
accts = accts.where(accts.ownership_type.isNotNull())

# select only needed columns
primary = primary.select('customer_key', 'branch_number', 'transaction_date', 'transaction_id')
direct = direct.select('customer_key', 'branch_number', 'transaction_date', 'transaction_id')
accts = accts.select('customer_key', 'date_opened', 'account_key', 'branch_id')

# calculate Primary Ownership Branch
primary = primary.where((primary.transaction_date >= one_year_ago) & (primary.transaction_date <= todays_date))
primary = primary.select('customer_key', 'branch_number', 'transaction_id')
primary = primary.groupBy(['customer_key', 'branch_number']).count().orderBy(['customer_key', 'branch_number'])
primary_window = Window.partitionBy(['customer_key']).orderBy(col('count').desc())
primary = primary.withColumn('row', row_number().over(primary_window)).filter(col('row') == 1).drop('row').drop('count')

# calculate Direct Ownership Branch
direct = direct.where((direct.transaction_date >= one_year_ago) & (direct.transaction_date <= todays_date))
direct = direct.select('customer_key', 'branch_number', 'transaction_id')
direct = direct.groupBy(['customer_key', 'branch_number']).count().orderBy(['customer_key', 'branch_number'])
direct_window = Window.partitionBy(['customer_key']).orderBy(col('count').desc())
direct = direct.withColumn('row', row_number().over(direct_window)).filter(col('row') == 1).drop('row').drop('count')

# calculate Branch of Most Recently Opened Account
accts = accts.where(accts.date_opened <= todays_date)
accts_window = Window.partitionBy(['customer_key']).orderBy(col('date_opened').desc())
accts = accts.withColumn('row', row_number().over(accts_window)).filter(col('row') == 1).drop('row')

# join primary, direct, and account branch calcs to the customer level
cust_data = cust_data.join(primary, 'customer_key', 'left').withColumnRenamed('branch_number', 'primary_branch_number')
cust_data = cust_data.join(direct, 'customer_key', 'left').withColumnRenamed('branch_number', 'direct_branch_number')
cust_data = cust_data.join(accts, 'customer_key', 'left').withColumnRenamed('branch_id', 'acct_branch_number')
cust_data = cust_data.select('customer_key', 'primary_branch_number', 'direct_branch_number', 'acct_branch_number', 'opening_branch')


# CALCULATE DYNAMIC BRANCH ASSIGNMENT

# based on logic cleared with *hidden* with slight modification
# added a backup of rm branch (opening_branch) so that we don't ever have a null dynamic branch assignment
# without this backup we would have nulls for people who are no longer customers and do not have
# any teller transactions in the last year nor do they have any xref records where they are direct
cust_data = cust_data.withColumn('dynamic_branch_assignment', when(cust_data.primary_branch_number.isNotNull(), cust_data.primary_branch_number).otherwise(when(cust_data.direct_branch_number.isNotNull(), cust_data.direct_branch_number).otherwise(when(cust_data.acct_branch_number.isNotNull(), cust_data.acct_branch_number).otherwise(cust_data.opening_branch))))


# FINISHING TOUCHES

# add utc_timestamp
cust_data = cust_data.withColumn('utc_timestamp', lit(todays_date))

# use prisma logic for is_direct flag -- direct owner of deposit or lending product
xref_direct = xref.where((xref.ownership_type == 'Direct') & ((xref.product_group == 'Deposit') | (xref.product_group == 'Lending')) & ((xref.conformed_status == 'OPEN') | (xref.conformed_status == 'DORMANT'))).dropDuplicates(['account_key', 'customer_key'])

# use prisma logic for is_active flag -- any open or dormant relationship in xref
is_active = xref.where((xref.conformed_status == 'OPEN') | (xref.conformed_status == 'DORMANT'))
is_active = xref.dropDuplicates(['account_key', 'customer_key'])

# create is_active and is_direct flags
is_active = is_active.select('customer_key').dropDuplicates()
is_active = is_active.withColumn('is_active', lit('Y'))
is_direct = xref_direct.select('customer_key').dropDuplicates()
is_direct = is_direct.withColumn('is_direct', lit('Y'))

# add is_active and is_direct flags to cust_data
cust_data = cust_data.join(is_active, 'customer_key', 'left')
cust_data = cust_data.join(is_direct, 'customer_key', 'left')
cust_data = cust_data.fillna('N', subset=['is_active', 'is_direct'])

# reorder columns
cust_data = cust_data.select('customer_key', 'primary_branch_number', 'direct_branch_number', 'acct_branch_number', 'opening_branch', 'dynamic_branch_assignment', 'is_active', 'is_direct', 'utc_timestamp')

# write file to lab for now -- will need to update this filepath for production
cust_data.coalesce(1).write.mode("overwrite").parquet(f"/mnt/*hidden*/{today}/")

# COMMAND ----------

# MAGIC %md
# MAGIC APPEND DAILY ASSIGNMENTS TO DELTA TABLE

# COMMAND ----------

# read todays assignments
branch_assignments = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")

# cast fields to conform to delta table schema
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
branch_assignments.write.mode("append").saveAsTable("dynamic_branch_assignments.dynamic_branch_assignments")
