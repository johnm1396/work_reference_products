# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta

# COMMAND ----------

trans = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/*/*/*/*.parquet')
trans = trans.withColumnRenamed("db/cr_flag", "db_cr_flag")
trans = trans.where(trans.transaction_code == '21')
trans = trans.where(~lower('description').like('%refund%'))
trans = trans.select('*', lit(1).alias("transaction_count"))
trans = trans.select('account_key', 'transaction_date', 'transaction_count', 'transaction_amount')
trans = trans.withColumn('transaction_date_month', date_format(col('transaction_date'), 'yyyy-MM-01')).groupBy(['account_key', 'transaction_date_month']).agg(sum('transaction_count').alias('transaction_count'), sum('transaction_amount').alias('transaction_amount'))

xref = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/*/*/*/*.parquet')
xref = xref.where(xref.primary_customer_flag == 'Y')
xref = xref.orderBy(xref.utc_timestamp.desc())
xref = xref.select('account_key', 'customer_key')
xref = xref.dropDuplicates(['account_key'])

trans = trans.join(xref, trans.account_key == xref.account_key, 'left')
trans = trans.select('customer_key', 'transaction_date_month', 'transaction_count', 'transaction_amount')
trans = trans.groupBy(['customer_key', 'transaction_date_month']).agg(sum('transaction_count').alias('transaction_count'), sum('transaction_amount').alias('transaction_amount'))

trans.coalesce(1).write.option("header", True).parquet('/mnt/*hidden*/John/ach_credits')

# COMMAND ----------

xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/*/*/*/*.parquet")

display(xref.where(xref.account_key == '*hidden*'))

# COMMAND ----------


