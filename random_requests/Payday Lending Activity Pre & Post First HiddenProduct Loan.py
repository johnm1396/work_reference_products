# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

trans = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/*/*/*/*.parquet')

# COMMAND ----------

trans = trans.withColumnRenamed("db/cr_flag", "db_cr_flag")

# COMMAND ----------

trans = trans.where("(description LIKE '%FAST PAYDAY%' OR description LIKE '%PAYDAY MONEY STORE%' OR description LIKE '%PAYDAY-LOAN%' OR description LIKE '%XPRESS PAYDAY%' OR description LIKE '%GETEZMONEY%' OR description LIKE '%EZMONEYCPA%' OR description LIKE '%FAST CASH NOW%' OR description LIKE '%SPEEDY CASH%' OR description LIKE '%SPEEDYCASH%' OR description LIKE '%Speedy #%' OR description LIKE '%ACE CASH EX%' OR description LIKE '%ADVANCE AMERICA%' OR description LIKE '%ADVANCEAMERICA%' OR description LIKE '%CASH CENTRAL%' OR description LIKE '%CASH POINT%' OR description LIKE '%CASHCALL%' OR description LIKE '%CASHNETUSA%' OR description LIKE '%CHECK N GO%' OR description LIKE '%GREEN GATE SERVICES%' OR description LIKE '%LENDGREEN%' OR description LIKE '%LENDUP%' OR description LIKE '%LOAN BY PHONE%' OR description LIKE '%MAXLEND%' OR description LIKE '%MOBILOANS%' OR description LIKE '%MONEYKEY%' OR description LIKE '%PLAIN GREEN%' OR description LIKE '%QC HOLDINGS%' OR description LIKE '%SPOTLOAN%' OR description LIKE '%SPOT LOAN%' OR description LIKE '%SURE ADVANCE%' OR description LIKE '%PLAIN GREEN LLC%' OR description LIKE '%NH CASH.COM%' OR description LIKE '%SNAP FINANCE%' OR description LIKE '%NORTH CASH%' OR description LIKE '%RADIANT CASH%' OR description LIKE '%GREENLINE LOANS%' OR description LIKE '%QUIK CASH%' OR description LIKE '%VBS HUMMINGBIRD%' OR description LIKE '%DOLLAR FINANCIAL GROUP%' OR description LIKE '%GREEN GATE SERVI%') and (description NOT LIKE '%ReliamaxLending%' OR description NOT LIKE '%SPEEDY CASH PAWN%' OR description NOT LIKE '%AUTO SNAP FINANCE%' OR description NOT LIKE '%CC-NORTH CASHI%')")

# COMMAND ----------

trans = trans.select('*', lit(1).alias("transaction_count"))

# COMMAND ----------

trans = trans.select('account_key', 'transaction_date', 'db_cr_flag', 'transaction_count', 'transaction_amount')

# COMMAND ----------

trans = trans.groupBy(['account_key', 'transaction_date', 'db_cr_flag']).agg(sum('transaction_count').alias('transaction_count'), sum('transaction_amount').alias('transaction_amount'))

# COMMAND ----------

# trans.write.saveAsTable("payday_lending_usage.agg_trans_data")
trans.coalesce(1).write.option("header", True).parquet('/mnt/*hidden*')

# COMMAND ----------


