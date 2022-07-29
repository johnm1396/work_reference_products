# Databricks notebook source
# MAGIC %md
# MAGIC Package Imports & File Path Variable Creation

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date, timedelta, datetime
import requests
import json
from requests.auth import HTTPBasicAuth
import getpass
import csv
import os

thirty_one_days_ago = date.today() - timedelta(31)
ninety_one_days_ago = date.today() - timedelta(91)
six_months_ago = date.today() - timedelta(182)

year = date.today().strftime("%Y")
month = date.today().strftime("%m")
day = date.today().strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

# MAGIC %md
# MAGIC Create DB and Master Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS *hidden*
# MAGIC LOCATION "/mnt/*hidden*/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS *hidden* (customer_key INT, exp_control_label STRING, date_added DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS *hidden* (customer_key INT, cp_model_group STRING, cp_model_date DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC Create Base Customer & Account Dataframe

# COMMAND ----------

# get all direct customer to acct relationships
*hidden* = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
*hidden* = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
*hidden* = *hidden*.join(*hidden*, 'relationship_key', 'left')
*hidden* = *hidden*.where(*hidden*.ownership_type == 'Direct')

# get all *hidden* account_keys from *hidden*
all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
all_prod = all_prod.where((all_prod.application_code == 'DD') | (all_prod.application_code == 'SV'))

# get conformed status from *hidden*
sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.where((sicod.code_type == 'STAT') | (sicod.code_type == 'SS') | (sicod.code_type == 'LNST') | (sicod.code_type == 'CLSD'))
sicod = sicod.where(sicod.application != 'SI')
sicod = sicod.withColumn('conformed_status', when(col('code_description_2') == '', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))
sicod = sicod.select('code_key', 'conformed_status')
sicod = sicod.dropDuplicates()

# join *hidden* to *hidden* to get account_key and conformed_status
all_prod = all_prod.join(sicod, all_prod.status_key == sicod.code_key, 'left')
accts = all_prod.select('account_key', 'conformed_status')

# join *hidden* and acct data together to get all direct customer to account relationships
cust_acct_data = *hidden*.join(accts, 'account_key', 'inner')

# filter to relationships where accounts are open
cust_acct_data = cust_acct_data.where(cust_acct_data.conformed_status == 'OPEN')

# COMMAND ----------

# MAGIC %md
# MAGIC Add Model Output to Customer & Account Dataframe

# COMMAND ----------

# get today's model output
prediction = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').withColumnRenamed("0", "prediction")

# clean up just in case
prediction = prediction.select(['account_key', 'prediction']).dropDuplicates()

# inner join prediction to the main customer/acct table
cust_acct_data = cust_acct_data.join(prediction, 'account_key', 'inner')

# COMMAND ----------

# MAGIC %md
# MAGIC Add Payday Lending Flag to the Customer & Account Dataframe

# COMMAND ----------

# get all accounts w/ payday lending activity in the prior 91 days and create a flag
payday_accts = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')
payday_accts = payday_accts.where(payday_accts.transaction_date >= ninety_one_days_ago)
payday_accts = payday_accts.where("(description LIKE '%FAST PAYDAY%' OR description LIKE '%PAYDAY MONEY STORE%' OR description LIKE '%PAYDAY-LOAN%' OR description LIKE '%XPRESS PAYDAY%' OR description LIKE '%GETEZMONEY%' OR description LIKE '%EZMONEYCPA%' OR description LIKE '%FAST CASH NOW%' OR description LIKE '%SPEEDY CASH%' OR description LIKE '%SPEEDYCASH%' OR description LIKE '%Speedy #%' OR description LIKE '%ACE CASH EX%' OR description LIKE '%ADVANCE AMERICA%' OR description LIKE '%ADVANCEAMERICA%' OR description LIKE '%CASH CENTRAL%' OR description LIKE '%CASH POINT%' OR description LIKE '%CASHCALL%' OR description LIKE '%CASHNETUSA%' OR description LIKE '%CHECK N GO%' OR description LIKE '%GREEN GATE SERVICES%' OR description LIKE '%LENDGREEN%' OR description LIKE '%LENDUP%' OR description LIKE '%LOAN BY PHONE%' OR description LIKE '%MAXLEND%' OR description LIKE '%MOBILOANS%' OR description LIKE '%MONEYKEY%' OR description LIKE '%PLAIN GREEN%' OR description LIKE '%QC HOLDINGS%' OR description LIKE '%SPOTLOAN%' OR description LIKE '%SPOT LOAN%' OR description LIKE '%SURE ADVANCE%' OR description LIKE '%PLAIN GREEN LLC%' OR description LIKE '%NH CASH.COM%' OR description LIKE '%SNAP FINANCE%' OR description LIKE '%NORTH CASH%' OR description LIKE '%RADIANT CASH%' OR description LIKE '%GREENLINE LOANS%' OR description LIKE '%QUIK CASH%' OR description LIKE '%VBS HUMMINGBIRD%' OR description LIKE '%DOLLAR FINANCIAL GROUP%' OR description LIKE '%GREEN GATE SERVI%') and (description NOT LIKE '%ReliamaxLending%' OR description NOT LIKE '%SPEEDY CASH PAWN%' OR description NOT LIKE '%AUTO SNAP FINANCE%' OR description NOT LIKE '%CC-NORTH CASHI%')")
payday_accts = payday_accts.select('account_key')
payday_accts = payday_accts.dropDuplicates()
payday_accts = payday_accts.withColumn('payday_lending_flag', lit(1))

# add payday_lending_flag to main dataset and fill null with zero to complete flag calc
cust_acct_data = cust_acct_data.join(payday_accts, 'account_key', 'left')
cust_acct_data = cust_acct_data.na.fill(value=0,subset=['payday_lending_flag'])

# COMMAND ----------

# MAGIC %md
# MAGIC Add NSF Flag to the Customer & Account Dataframe

# COMMAND ----------

# get all accounts w/ nsf activity in the prior 31 days and create a flag
nsf_accts = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/*/*/*/*.parquet')
nsf_accts = nsf_accts.where(nsf_accts.transaction_date >= thirty_one_days_ago)
nsf_accts = nsf_accts.where(nsf_accts.transaction_code==71)
nsf_accts = nsf_accts.select('account_key')
nsf_accts = nsf_accts.dropDuplicates()
nsf_accts = nsf_accts.withColumn('nsf_flag', lit(1))

# add nsf_flag to main dataset and fill null with zero to complete flag calc
cust_acct_data = cust_acct_data.join(nsf_accts, 'account_key', 'left')
cust_acct_data = cust_acct_data.na.fill(value=0,subset=['nsf_flag'])

# COMMAND ----------

# MAGIC %md
# MAGIC Add Decreasing Balance Flag to the Customer & Account Dataframe

# COMMAND ----------

# get all accounts in cluster 0 (decreasing balance) from daily segmentation clustering and create a flag
decreasing_balance_accts = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
decreasing_balance_accts = decreasing_balance_accts.where(decreasing_balance_accts.cluster == 0)
decreasing_balance_accts = decreasing_balance_accts.select('account_key')
decreasing_balance_accts = decreasing_balance_accts.dropDuplicates()
decreasing_balance_accts = decreasing_balance_accts.withColumn('decreasing_balance_flag', lit(1))

# add decreasing_balance_flag to main dataset and fill null with zero to complete flag calc
cust_acct_data = cust_acct_data.join(decreasing_balance_accts, 'account_key', 'left').select(['customer_key', 'ownership_type', 'account_key', 'conformed_status', 'prediction', 'payday_lending_flag', 'nsf_flag', 'decreasing_balance_flag'])
cust_acct_data = cust_acct_data.na.fill(value=0,subset=['decreasing_balance_flag'])

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Customers w/ 3 or more CP Loans in the prior six months from the Customer & Account Dataframe

# COMMAND ----------

# get number of *hidden* in prior 6 months
num_cp_loans = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
num_cp_loans = num_cp_loans.where(num_cp_loans.product_code=='*hidden*')
num_cp_loans = *hidden*.join(num_cp_loans, 'account_key', 'inner')
num_cp_loans = num_cp_loans.where(num_cp_loans.boarding_date>=six_months_ago)
num_cp_loans = num_cp_loans.select(['customer_key', 'account_key'])
num_cp_loans = num_cp_loans.groupBy(['customer_key']).agg(count('account_key').alias('num_cp_loans'))

# add num_cp_loans to main dataset and fill null with zero
cust_acct_data = cust_acct_data.join(num_cp_loans, 'customer_key', 'left').select(['customer_key', 'account_key', 'prediction', 'payday_lending_flag', 'nsf_flag', 'decreasing_balance_flag', 'num_cp_loans'])
cust_acct_data = cust_acct_data.na.fill(value=0,subset=['num_cp_loans'])

# drop customers with 3 or more CP loans in prior 6 months since they do not qualify and drop num_cp_loans
cust_acct_data = cust_acct_data.where(cust_acct_data.num_cp_loans < 3)
cust_acct_data = cust_acct_data.select(['customer_key', 'account_key', 'prediction', 'payday_lending_flag', 'nsf_flag', 'decreasing_balance_flag'])

# COMMAND ----------

# MAGIC %md
# MAGIC Split the Customer & Account Dataframe into Negative and Positive Prediction Dataframes

# COMMAND ----------

# split data into negative and positive predictions
negative_predictions = cust_acct_data.where(cust_acct_data.prediction == '0')
positive_predictions = cust_acct_data.where(cust_acct_data.prediction == '1')

# cleanse and agg() to customer level for negative_predictions 
negative_predictions = negative_predictions.select(['customer_key', 'payday_lending_flag', 'nsf_flag', 'decreasing_balance_flag'])
negative_predictions = negative_predictions.groupBy(['customer_key']).agg(max('payday_lending_flag').alias('payday_lending_flag'), max('nsf_flag').alias('nsf_flag'), max('decreasing_balance_flag').alias('decreasing_balance_flag'))

# cleanse and agg() to customer level for positive_predictions
positive_predictions = positive_predictions.select(['customer_key', 'payday_lending_flag', 'nsf_flag', 'decreasing_balance_flag'])
positive_predictions = positive_predictions.groupBy(['customer_key']).agg(max('payday_lending_flag').alias('payday_lending_flag'), max('nsf_flag').alias('nsf_flag'), max('decreasing_balance_flag').alias('decreasing_balance_flag'))

# create group flag for negative_predictions, drop excess columns and leading zeros from customer_key
negative_predictions = negative_predictions.withColumn('cp_model_group', when(negative_predictions.payday_lending_flag == '1', 'Payday').otherwise(when(negative_predictions.nsf_flag == '1', 'NSF').otherwise(when(negative_predictions.decreasing_balance_flag == '1', 'Decreasing Balance').otherwise('General')))).select(['customer_key', 'cp_model_group'])
negative_predictions = negative_predictions.withColumn("customer_key", negative_predictions["customer_key"].cast('integer'))

# create group flag for positive_predictions, drop excess columns and leading zeros from customer_key
positive_predictions = positive_predictions.withColumn('cp_model_group', when(positive_predictions.payday_lending_flag == '1', 'Payday').otherwise(when(positive_predictions.nsf_flag == '1', 'NSF').otherwise(when(positive_predictions.decreasing_balance_flag == '1', 'Decreasing Balance').otherwise('General')))).select(['customer_key', 'cp_model_group'])
positive_predictions = positive_predictions.withColumn("customer_key", positive_predictions["customer_key"].cast('integer'))

# COMMAND ----------

# MAGIC %md
# MAGIC Update the exp_or_control_master table with labels for New Customers (customers who have never been in the model output before)

# COMMAND ----------

# read in our master lookup table
exp_or_control_master = spark.table('*hidden*')

# COMMAND ----------

# get current day positive predictions that don't exist in the master table
new_predictions = positive_predictions.join(exp_or_control_master, 'customer_key', 'leftanti')

# split todays new_predictions into experiment and control groups
control_group = new_predictions.sampleBy('cp_model_group', fractions={'General': 0.15, 'NSF': 0.15, 'Payday': 0.15, 'Decreasing Balance': 0.15}, seed=42)
experiment_group = new_predictions.join(control_group, 'customer_key', 'leftanti')

# label and union new customers into master lookup table
# added datestamp to the master table so we know when the customer first appeared in the model output
# this will also allow us to drop customers from the table after a certain time if we wanted to reset control groups after n days
control_group = control_group.withColumn('exp_control_label', lit('Control')).withColumn('date_added', lit(date.today())).select('customer_key', 'exp_control_label', 'date_added')
experiment_group = experiment_group.withColumn('exp_control_label', lit('Experiment')).withColumn('date_added', lit(date.today())).select('customer_key', 'exp_control_label', 'date_added')
exp_or_control_master = exp_or_control_master.union(experiment_group)
exp_or_control_master = exp_or_control_master.union(control_group)

# overwrite master table with complete lookup data
exp_or_control_master.write.mode("overwrite").saveAsTable("*hidden*")

# COMMAND ----------

# MAGIC %md
# MAGIC Create the dataframe we will upload to *hidden* via API post

# COMMAND ----------

# create daily daily_output by removing control group from positive_predictions
daily_output = positive_predictions.join(exp_or_control_master, 'customer_key', 'left')
daily_output = daily_output.where(daily_output.exp_control_label == 'Experiment').select('customer_key', 'cp_model_group')
daily_output = daily_output.withColumn('cp_model_date', lit(date.today())).select('customer_key', 'cp_model_group', 'cp_model_date')

# overwrite daily_output delta table with new values
daily_output.write.mode("overwrite").saveAsTable("*hidden*")

# COMMAND ----------

# MAGIC %md
# MAGIC Write dataframes to daily snapshot file

# COMMAND ----------

# Save positive predictions to gold location
positive_predictions.coalesce(1).write.mode("overwrite").parquet(f"/mnt/*hidden*/positive_predictions/{today}")

# Save negative predictions to gold location
negative_predictions.coalesce(1).write.mode("overwrite").parquet(f"/mnt/*hidden*/negative_predictions/{today}")

# Save daily_output as parquet to gold location
daily_output.coalesce(1).write.mode("overwrite").parquet(f"/mnt/*hidden*/daily_output/{today}")

# Save daily_output as csv to gold location for upload to prisma
daily_output_pd = daily_output.toPandas()
daily_output_pd.to_csv(f"/dbfs/mnt/*hidden*/{today}/"+"daily_output.csv", index=False, encoding="utf-8")

# COMMAND ----------

# MAGIC %md
# MAGIC Post the daily_output to the *hidden* campaign models

# COMMAND ----------

# Post to Campaign Data Model for: *hidden* Predictive Model - Non-Digital Email|*hidden*|b6l1cpdcv7
def post(file_name):
    url = "*hidden*"
    user = 'api'
    pword = '*hidden*'
    with open(file_name, 'rb') as file:
        files=[('file',('*hidden*',file,'text/csv'))]
        headers = {'Authorization': '*hidden*'}
        response = requests.post(url, auth = HTTPBasicAuth(user, pword) , files=files)
                                   
post(f"/dbfs/mnt/*hidden*/{today}/daily_output.csv")

# COMMAND ----------

# Post to Campaign Data Model for: *hidden* Predictive Model - Digital Email|*hidden*|b6l1cpdcv7
def post(file_name):
    url = "*hidden*"
    user = 'api'
    pword = '*hidden*'
    with open(file_name, 'rb') as file:
        files=[('file',('*hidden*',file,'text/csv'))]
        headers = {'Authorization': '*hidden*'}
        response = requests.post(url, auth = HTTPBasicAuth(user, pword) , files=files)
                                   
post(f"/dbfs/mnt/*hidden*/{today}/daily_output.csv")

# COMMAND ----------

# Post to Campaign Data Model for: *hidden* Predictive Model - Digital Ads|*hidden*|b6l1cpdcv7
def post(file_name):
    url = "*hidden*"
    user = 'api'
    pword = '*hidden*'
    with open(file_name, 'rb') as file:
        files=[('file',('*hidden*',file,'text/csv'))]
        headers = {'Authorization': '*hidden*'}
        response = requests.post(url, auth = HTTPBasicAuth(user, pword) , files=files)
                                   
post(f"/dbfs/mnt/*hidden*/{today}/daily_output.csv")

# COMMAND ----------

print(daily_output.count())

# COMMAND ----------

display(daily_output)

# COMMAND ----------


