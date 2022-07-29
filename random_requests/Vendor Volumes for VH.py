# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

# COMMAND ----------

file = "/mnt/*hidden*/2021/*/*/*.parquet"

trans = spark.read.option("header", True).option("inferSchema", True).parquet(f"{file}")
trans = trans.withColumnRenamed("db/cr_flag", "db_cr_flag")

trans = trans.where((lower('description').like('%purch sq%')) | (lower('description').like('%gosq.com%')) | (lower('description').like('%paypal%')) | (lower('description').like('%square inc%')) | (lower('description').like('%squareup.com%')) | (lower('description').like('%intuit%')) | (lower('description').like('%quickbooks%')) | (lower('description').like('%stripe.com%')) | (lower('description').like('%stripe/%')) | (lower('description').like('%venmo%')))

trans = trans.where(~lower('description').like('%square enix%'))
trans = trans.where(~lower('description').like('%intuitive%'))
trans = trans.where(~lower('description').like('%intuition%'))

trans = trans.withColumn('trans_type_flag', when(lower('description').like('%paypal%'), 'PayPal').otherwise(when(lower('description').like('%intuit%'), 'Intuit').otherwise(when(lower('description').like('%quickbooks%'), 'Quickbooks').otherwise(when(lower('description').like('%venmo%'), 'Venmo').otherwise(when(lower('description').like('%stripe.com%'), 'Stripe').otherwise(when(lower('description').like('%stripe/%'), 'Stripe').otherwise('Square')))))))

trans = trans.select('*', lit(1).alias("transaction_count"))
trans = trans.select('account_key', 'transaction_date', 'trans_type_flag', 'db_cr_flag', 'transaction_count', 'transaction_amount')

trans = trans.groupBy(['account_key', 'transaction_date', 'trans_type_flag', 'db_cr_flag']).agg(sum('transaction_count').alias('transaction_count'), sum('transaction_amount').alias('transaction_amount'))

trans.coalesce(1).write.option("header", True).parquet('/mnt/*hidden*')

# COMMAND ----------


