# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# COMMAND ----------

# CREATE USER_BRANCH FROM TELLER TRANSACTIONS
user_branch = spark.read.option("header", True).option("inferSchema", True).parquet("/mnt/*hidden*/*/*/*/*.parquet")
user_branch_w_date = user_branch.select('user_id', 'transaction_date', 'branch_number', 'transaction_id')
user_branch_w_date = user_branch_w_date.groupBy(['user_id', 'transaction_date', 'branch_number']).count().orderBy(['user_id', 'transaction_date', 'branch_number'])
window = Window.partitionBy(['user_id', 'transaction_date']).orderBy(col('count').desc())
user_branch_w_date = user_branch_w_date.withColumn('row', row_number().over(window)).filter(col('row') == 1).drop('row').drop('count')

user_branch_no_date = user_branch.select('user_id', 'branch_number', 'transaction_id')
user_branch_no_date = user_branch_no_date.groupBy(['user_id', 'branch_number']).count().orderBy(['user_id', 'branch_number'])
window = Window.partitionBy(['user_id']).orderBy(col('count').desc())
user_branch_no_date = user_branch_no_date.withColumn('row', row_number().over(window)).filter(col('row') == 1).drop('row').drop('count')
user_branch_no_date = user_branch_no_date.withColumnRenamed('user_id',"user_id_v2").withColumnRenamed('branch_number',"branch_number_v2")

# COMMAND ----------

# CREATE VIEW_LOG_COMPLETE
session = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
session = session.select('LOGGING SESSION ID', 'USER ID', 'CREATE DATE', 'CLIENT IP', 'SOURCE', 'SECURITY GROUPS', 'EXCEPTION INDICATOR', 'Bank ID')
view = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
view = view.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'VIEW CREATE DATE', 'VIEW NAME', 'VIEW DESC', 'CLOSE DATE', 'Activity Indicator', 'Maintenance Indicator')
session_view = session.join(view, 'LOGGING SESSION ID', 'inner')
view_activity = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
view_activity = view_activity.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'APPLICATION', 'PRIMARY_KEY', 'SECONDARY_KEY', 'LOGGING_FUNCTION_ID', 'FUNCTION CREATE DATE', 'VIEW TYPE', 'PRIMARY_KEY ID', 'CORE TABLE', 'EN_LOGGINGSESSIONVIEWCUSTOMER Count')
view_log_complete = session_view.join(view_activity, ['LOGGING SESSION ID', 'LOGGING VIEW ID'], 'inner')
view_log_complete = view_log_complete.withColumn('DATE', to_date(col('CREATE DATE'), 'yyyy-MM-dd'))

del session
del view
del session_view
del view_activity

# COMMAND ----------

# join w/ the 'most accurate' branch classification
view_log_complete = view_log_complete.join(user_branch_w_date, [(view_log_complete['USER ID'] == user_branch_w_date.user_id)&(view_log_complete.DATE == user_branch_w_date.transaction_date)], 'left')

# COMMAND ----------

# add the less accurate data
view_log_complete = view_log_complete.join(user_branch_no_date, view_log_complete['USER ID'] == user_branch_no_date.user_id_v2, 'left')

# COMMAND ----------

display(view_log_complete)

# COMMAND ----------

view_log_complete = view_log_complete.withColumn('user_branch_number',  when(view_log_complete.branch_number.isNull(), view_log_complete.branch_number_v2).otherwise(view_log_complete.branch_number))

# COMMAND ----------

view_log_complete = view_log_complete.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'USER ID', 'CREATE DATE', 'CLIENT IP', 'SOURCE', 'SECURITY GROUPS', 'EXCEPTION INDICATOR', 'Bank ID', 'VIEW CREATE DATE', 'VIEW NAME', 'VIEW DESC', 'CLOSE DATE', 'Activity Indicator', 'Maintenance Indicator', 'APPLICATION', 'PRIMARY_KEY', 'SECONDARY_KEY', 'LOGGING_FUNCTION_ID', 'FUNCTION CREATE DATE', 'VIEW TYPE', 'PRIMARY_KEY ID', 'CORE TABLE', 'EN_LOGGINGSESSIONVIEWCUSTOMER Count', 'user_branch_number')

# COMMAND ----------

null_branch = view_log_complete.where(view_log_complete.user_branch_number.isNull())

# COMMAND ----------

null_branch = null_branch.select('USER ID', 'EN_LOGGINGSESSIONVIEWCUSTOMER Count')

# COMMAND ----------

null_branch = null_branch.groupBy('USER ID').sum()

# COMMAND ----------

display(null_branch)

# COMMAND ----------

# CREATE UPDATE_LOG_COMPLETE
update_transactions = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
update_transactions = update_transactions.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'USER ID', 'CREATE DATE', 'VIEW_CREATE_DATE', 'OVERRIDE USER ID', 'EXECUTION DATE', 'OVERRIDE COMMENT', 'APPROVER COMMENT', 'PAGE NAME', 'Bank ID')
updates = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
updates = updates.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'UPDATE ID', 'APPLICATION AREA', 'TABLE NAME', 'TABLE SUMMARY', 'ACTION', 'ACTION Description', 'PROGRAM NAME')
update_trans_updates = update_transactions.join(updates, ['LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID'], 'inner')
update_acct_cust = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
update_acct_cust = update_acct_cust.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'UPDATE ID', 'CLIENT IP', 'APPLICATION', 'PRIMARY KEY', 'SECONDARY KEY', 'ACCOUNT CUSTOMER KEY', 'CORE_TABLE')
update_trans_updates_acct_cust = update_trans_updates.join(update_acct_cust, ['LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'UPDATE ID'], 'leftouter')
update_key = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
update_key = update_key.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'UPDATE ID', 'COLUMN NAME', 'COLUMN VALUE')
update_trans_updates_acct_cust_key = update_trans_updates_acct_cust.join(update_key, ['LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'UPDATE ID'], 'inner')
update_value = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/*hidden*/*.csv")
update_value = update_value.select('LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'UPDATE ID', 'COLUMN NAME', 'ORIGINAL COLUMN VALUE', 'NEW COLUMN VALUE', 'COLUMN INFORMATION', 'EN_LOGGINGSESSIONVIEWUPDATEVALUE Count')
update_log_complete = update_trans_updates_acct_cust_key.join(update_value, ['LOGGING SESSION ID', 'LOGGING VIEW ID', 'TRANSACTION ID', 'UPDATE ID'], 'inner')
update_log_complete = update_log_complete.withColumn('DATE', to_date(col('CREATE DATE'), 'yyyy-MM-dd'))

del update_transactions
del updates
del update_trans_updates
del update_acct_cust
del update_trans_updates_acct_cust
del update_key
del update_trans_updates_acct_cust_key
del update_value

display(update_log_complete)

# COMMAND ----------

update_log_complete = update_log_complete.join(user_branch, [], 'left')
