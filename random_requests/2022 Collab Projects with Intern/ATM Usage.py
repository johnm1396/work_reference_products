# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType, StringType
import pyspark.sql.functions as F

todays_date = date.today()

year = todays_date.strftime("%Y")
month = todays_date.strftime("%m")
day = todays_date.strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

atm_df = spark.table('curated_transactions_debit_card.curated_debit_card_transactions')

# COMMAND ----------

# create product and card xref dfs
xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/*/*/*/*.parquet").orderBy('utc_timestamp', ascending=False).dropDuplicates(subset=['customer_key', 'account_key'])

rmrel = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('relationship_key', 'ownership_type').dropDuplicates()
xref = xref.join(rmrel, 'relationship_key', 'left')
xref = xref.where(xref['ownership_type']=='Direct')
xref = xref.withColumn('customer_key', xref.customer_key.cast(IntegerType()))

card_xref = xref.where((xref.application=='CA') & (xref.primary_customer_flag=='Y'))
card_xref = card_xref.withColumn('card_number', F.regexp_replace('account_number', r'^[0]*', ''))
card_xref = card_xref.withColumn('customer_key', F.regexp_replace('customer_key', r'^[0]*', ''))
card_xref = card_xref.withColumn("pan",expr("substring(card_number, 1, length(card_number)-1)"))
card_xref = card_xref.select('customer_key','pan')
card_xref = card_xref.withColumn('pan', card_xref['pan'].cast('string'))

product_xref = xref.where(xref.application.isin('CD', 'DD', 'IR', 'SV', 'LN', 'ML'))
product_xref = product_xref.select('account_key', 'customer_key')

del xref

# COMMAND ----------

# create atm transaction df and clean it up
atm_df = spark.table('curated_transactions_debit_card.curated_debit_card_transactions')

atm_df = atm_df.where((atm_df['transaction_type_description']=='TRANSFER') | (atm_df['transaction_type_description']=='CHECKS ONLY DEPOSIT')| (atm_df['transaction_type_description']=='CASH ONLY DEPOSIT')| (atm_df['transaction_type_description']=='WITHDRAWAL')| (atm_df['transaction_type_description']=='INQUIRY'))

atm_df = atm_df.where(atm_df['merchant_name']=='*hidden*')

atm_df = atm_df.where(atm_df['card_acceptor_terminal_id']!='99999999')

atm_df = atm_df.withColumn('card_acceptor_terminal_location', when(atm_df.card_acceptor_terminal_location.contains('*hidden*'), expr("substring(card_acceptor_terminal_location, 1, length(card_acceptor_terminal_location)-2)")).otherwise(atm_df.card_acceptor_terminal_location))

atm_df = atm_df.withColumn('pan', atm_df['pan'].cast('string'))

# add customer_key to join to customer data
atm_df = atm_df.join(card_xref, on='pan', how='left')

# write to delta table
atm_df.write.format('delta').mode('overwrite').saveAsTable('power_bi_data_products.atm_usage_atm_transactions')

# COMMAND ----------

# build customer df
customer_data = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/*/*/*/*.parquet").orderBy('utc_timestamp', ascending=False).dropDuplicates(subset=['customer_key']).select('customer_key', 'customer_type', 'short_name')
customer_data = customer_data.withColumn('customer_key', customer_data.customer_key.cast(IntegerType()))
customer_data = customer_data.withColumnRenamed('short_name','customer_name')

# add primary_atm
primary_atm = atm_df.where(atm_df['calibrated_transaction_date']>=(date.today()-timedelta(90)))
primary_atm = primary_atm.select('customer_key','card_acceptor_terminal_id')
primary_atm = primary_atm.groupBy(['customer_key','card_acceptor_terminal_id']).count().orderBy('count', ascending=False)
primary_atm = primary_atm.dropDuplicates(subset=['customer_key'])
primary_atm = primary_atm.withColumnRenamed('card_acceptor_terminal_id','primary_atm')
primary_atm = primary_atm.select('customer_key','primary_atm')

customer_data = customer_data.join(primary_atm, on='customer_key', how='left')

# add dynamic_branch_name
dynamic_branches_df = spark.table('dynamic_branch_assignments.dynamic_branch_assignments')
dynamic_branches_df = dynamic_branches_df.orderBy('utc_timestamp', ascending=False)
dynamic_branches_df = dynamic_branches_df.dropDuplicates(subset=['customer_key'])
dynamic_branches_df = dynamic_branches_df.select('customer_key', 'dynamic_branch_name')

customer_data = customer_data.join(dynamic_branches_df, on='customer_key', how='left')

# add balances
all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet")

deposits = all_prod.where(all_prod.application_code.isin('CD', 'DD', 'IR', 'SV')).select('account_key', 'average_balance')
deposits = deposits.join(product_xref, on='account_key', how='left')
deposits = deposits.select('customer_key','average_balance')
deposits = deposits.groupBy('customer_key').agg(sum('average_balance').alias('average_deposit_balances'))

lending = all_prod.where(all_prod.application_code.isin('LN', 'ML')).select('account_key', 'book_balance')
lending = lending.join(product_xref, on='account_key', how='left')
lending = lending.select('customer_key','book_balance')
lending = lending.groupBy('customer_key').agg(sum('book_balance').alias('average_lending_balances'))

customer_data = customer_data.join(deposits, on='customer_key', how='left')
customer_data = customer_data.join(lending, on='customer_key', how='left')

# write to delta table
customer_data.write.format('delta').mode('overwrite').saveAsTable('power_bi_data_products.atm_usage_customer_data')

