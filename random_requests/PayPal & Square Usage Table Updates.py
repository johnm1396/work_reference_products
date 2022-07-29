# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

year = date.today().strftime("%Y")
month = date.today().strftime("%m")
day = date.today().strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day + "/" + "*.parquet")

# COMMAND ----------

file = "/mnt/*hidden*/"

trans = spark.read.option("header", True).option("inferSchema", True).parquet(f"{file}{today}")
trans = trans.withColumnRenamed("db/cr_flag", "db_cr_flag")

trans = trans.where((lower('description').like('%purch sq%')) | (lower('description').like('%gosq.com%')) | (lower('description').like('%paypal%')) | (lower('description').like('%square inc%')) | (lower('description').like('%squareup.com%')))

trans = trans.where(~lower('description').like('%square enix%'))
trans = trans.withColumn('trans_type_flag', when(lower('description').like('%paypal%'), 'PayPal').otherwise('Square'))
trans = trans.select('*', lit(1).alias("transaction_count"))
trans = trans.select('transaction_date', 'account_key', 'trans_type_flag', 'db_cr_flag', 'transaction_count', 'transaction_amount')
trans = trans.groupBy(['transaction_date', 'account_key', 'trans_type_flag', 'db_cr_flag']).agg(sum('transaction_count').alias('transaction_count'), sum('transaction_amount').alias('transaction_amount'))
trans.write.mode("overwrite").saveAsTable("paypal_square_usage.trans_data")

# COMMAND ----------

file = "/mnt/*hidden*/"
custs = spark.read.option("header", True).option("inferSchema", True).parquet(f"{file}{today}")
custs.write.mode("overwrite").saveAsTable("paypal_square_usage.cust_data")

# COMMAND ----------

file = "/mnt/*hidden*/"
xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"{file}{today}")
xref = xref.select('account_key', 'ownership_type_code', 'ownership_type', 'relationship_type_code', 'relationship_type', 'primary_customer_flag', 'customer_key')
xref.write.mode("overwrite").saveAsTable("paypal_square_usage.xref_data")

# COMMAND ----------

file = "/mnt/*hidden*/"
accts = spark.read.option("header", True).option("inferSchema", True).parquet(f"{file}{today}")
accts = accts.select('account_key', 'account_type_code', 'conformed_status', 'application', 'account_type','current_balance', 'branch', 'officer_1_code', 'officer_1_name', 'gl_type_code', 'gl_type', 'date_opened', 'date_closed', 'overdrawn_items_ltd', 'overdraft_charges_ltd', 'nsf_items_ltd', 'interest_paid_previous_year', 'interest_paid_ytd', 'interest_amount_last_paid', 'average_balance_ytd', 'average_balance_mtd')
accts.write.mode("overwrite").saveAsTable("paypal_square_usage.acct_data")

# COMMAND ----------

file = "/mnt/*hidden*/*/*/*/*.parquet"

trans = spark.read.option("header", True).option("inferSchema", True).parquet(f"{file}")
trans = trans.withColumnRenamed("db/cr_flag", "db_cr_flag")
trans = trans.where((lower('description').like('%purch sq%')) | (lower('description').like('%gosq.com%')) | (lower('description').like('%paypal%')) | (lower('description').like('%square inc%')) | (lower('description').like('%squareup.com%')))
trans.count()

# COMMAND ----------