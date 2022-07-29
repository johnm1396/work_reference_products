# Databricks notebook source
from pyspark.sql.functions import *
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, DateType, DoubleType, StringType
import pyspark.pandas as ps

# date variables
todays_date = date.today()

# date constructs for filepaths
year = todays_date.strftime("%Y")
month = todays_date.strftime("%m")
day = todays_date.strftime("%d")
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

#xref filtered by ownership
xref = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").orderBy('utc_timestamp', ascending=False).dropDuplicates(['account_key', 'customer_key'])

rmrel = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet').select('relationship_key', 'ownership_type').dropDuplicates()

xref = xref.join(rmrel, 'relationship_key', 'left')

xref = xref.filter(xref.ownership_type == 'Direct')

#don't need drop duplicates
xref = xref.select('account_key', 'customer_key').dropDuplicates()

# COMMAND ----------

df = spark.read.option("header", True).option("inferSchema", True).parquet(f"/mnt/*hidden*/{today}/*.parquet").select('account_key','delivery_method','form_category')
df = df.where(df['delivery_method']=="E")
df = df.where(df["form_category"]=="STMT")
df = df.withColumn('estatements', lit(1))

# COMMAND ----------

#df with all account level data
products = spark.table("retail_digital_analysis_db.product_relationship").select("product_key_pck", "product_group_pck","application_code_pck")

all_prod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('account_key','product_key', 'status_key')

consumer_business = spark.table('retail_digital_analysis_db.consumer_business_lookup').select('product_key','consumer_or_business_product' )

sicod = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet')
sicod = sicod.where((sicod.code_type=='STAT')|(sicod.code_type=='SS')|(sicod.code_type=='LNST')|(sicod.code_type=='CLSD'))
sicod = sicod.where(sicod.application!='SI')
sicod = sicod.select('code_key', 'utc_timestamp', 'code_description_2').dropDuplicates()


all_prod_e = all_prod.join(df, on='account_key', how='left').join(products, all_prod['product_key']==products['product_key_pck'], how='left').join(consumer_business, on='product_key', how="left").join(sicod, all_prod['status_key']==sicod['code_key'], how='left')

all_prod_e = all_prod_e.withColumn('conformed_status', when(col('code_description_2')=='', 'OPEN').otherwise(when(col('code_description_2').isNull(), 'OPEN').otherwise(col('code_description_2'))))


# COMMAND ----------

#filtered down account level data
all_prod_e = all_prod_e.where(all_prod_e['conformed_status'].isin("OPEN","DORMANT"))
all_prod_e = all_prod_e.where(all_prod_e['product_group_pck'].isin("Deposit","Lending"))
#all_prod_e = all_prod_e.withColumn('eligible_product', lit(1))

# COMMAND ----------

display(all_prod_e)
