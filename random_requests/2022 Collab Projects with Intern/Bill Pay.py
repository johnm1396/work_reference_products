# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType, DoubleType, StringType

# date variables
todays_date = date.today()

# date constructs for filepaths
year = todays_date.strftime("%Y")
month = todays_date.strftime("%m")
day = todays_date.strftime("%d")

# COMMAND ----------

#these need updated once files are in the cloud

transactions = spark.read.option("header", True).option("inferSchema", True).csv(f'/mnt/*hidden*/MonthlyTransactions.csv')

subscribers = spark.read.option("header", True).option("inferSchema", True).csv(f'/mnt/*hidden*/SubscriberStatistics.csv')
subscribers = subscribers.withColumn("Date Last Transaction Processed",( to_date(col("Date Last Transaction Processed"), "M/d/yyyy")))
subscribers = subscribers.withColumn("Enrollment Date",( to_date(col("Enrollment Date"), "M/d/yyyy")))

# COMMAND ----------

subscribers = subscribers.withColumn('active_30', when(date.today()-timedelta(30)<subscribers['Date Last Transaction Processed'], 'yes').otherwise("no"))

# COMMAND ----------


