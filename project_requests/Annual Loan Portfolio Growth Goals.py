# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# get 2016 ln/ml portfolio:
# get 2016 ln portfolio
ln_port_ye_2016 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/*/*/*.parquet')
ln_port_ye_2016 = ln_port_ye_2016.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ln_port_ye_2016 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2016/*/*/*.parquet')
raw_ln_port_ye_2016 = raw_ln_port_ye_2016.where(raw_ln_port_ye_2016.participation_number==0)
raw_ln_port_ye_2016 = raw_ln_port_ye_2016.select(['account_key', 'utc_timestamp', 'original_loan_amount']).dropDuplicates()
ln_port_ye_2016 = ln_port_ye_2016.join(raw_ln_port_ye_2016, ['account_key', 'utc_timestamp'], 'left')
ln_port_ye_2016 = ln_port_ye_2016.where(ln_port_ye_2016.gl_type.startswith('Retail'))
ln_port_ye_2016 = ln_port_ye_2016.select(['account_key', 'utc_timestamp', 'boarding_date', 'original_loan_amount', 'bank_owned_principle_assets'])

# get 2016 ml portfolio
ml_port_ye_2016 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2016/*/*/*.parquet')
ml_port_ye_2016 = ml_port_ye_2016.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ml_port_ye_2016 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2016/*/*/*.parquet')
raw_ml_port_ye_2016 = raw_ml_port_ye_2016.select(['account_key', 'utc_timestamp', 'orig_principal_balance'])
ml_port_ye_2016 = ml_port_ye_2016.join(raw_ml_port_ye_2016, ['account_key', 'utc_timestamp'], 'left')
ml_port_ye_2016 = ml_port_ye_2016.where(ml_port_ye_2016.gl_type.startswith('Retail'))
ml_port_ye_2016 = ml_port_ye_2016.select(['account_key', 'utc_timestamp', 'boarding_date', 'orig_principal_balance', 'bank_owned_principle_assets'])

# union ln and ml together
ye_2016 = ln_port_ye_2016.union(ml_port_ye_2016)


# get 2017 ln/ml portfolio:
# get 2017 ln portfolio
ln_port_ye_2017 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2017/*/*/*.parquet')
ln_port_ye_2017 = ln_port_ye_2017.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ln_port_ye_2017 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2017/*/*/*.parquet')
raw_ln_port_ye_2017 = raw_ln_port_ye_2017.where(raw_ln_port_ye_2017.participation_number==0)
raw_ln_port_ye_2017 = raw_ln_port_ye_2017.select(['account_key', 'utc_timestamp', 'original_loan_amount']).dropDuplicates()
ln_port_ye_2017 = ln_port_ye_2017.join(raw_ln_port_ye_2017, ['account_key', 'utc_timestamp'], 'left')
ln_port_ye_2017 = ln_port_ye_2017.where(ln_port_ye_2017.gl_type.startswith('Retail'))
ln_port_ye_2017 = ln_port_ye_2017.select(['account_key', 'utc_timestamp', 'boarding_date', 'original_loan_amount', 'bank_owned_principle_assets'])

# get 2017 ml portfolio
ml_port_ye_2017 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2017/*/*/*.parquet')
ml_port_ye_2017 = ml_port_ye_2017.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ml_port_ye_2017 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2017/*/*/*.parquet')
raw_ml_port_ye_2017 = raw_ml_port_ye_2017.select(['account_key', 'utc_timestamp', 'orig_principal_balance'])
ml_port_ye_2017 = ml_port_ye_2017.join(raw_ml_port_ye_2017, ['account_key', 'utc_timestamp'], 'left')
ml_port_ye_2017 = ml_port_ye_2017.where(ml_port_ye_2017.gl_type.startswith('Retail'))
ml_port_ye_2017 = ml_port_ye_2017.select(['account_key', 'utc_timestamp', 'boarding_date', 'orig_principal_balance', 'bank_owned_principle_assets'])

# union ln and ml together
ye_2017 = ln_port_ye_2017.union(ml_port_ye_2017)


# get 2018 ln/ml portfolio:
# get 2018 ln portfolio
ln_port_ye_2018 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2018/*/*/*.parquet')
ln_port_ye_2018 = ln_port_ye_2018.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ln_port_ye_2018 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2018/*/*/*.parquet')
raw_ln_port_ye_2018 = raw_ln_port_ye_2018.where(raw_ln_port_ye_2018.participation_number==0)
raw_ln_port_ye_2018 = raw_ln_port_ye_2018.select(['account_key', 'utc_timestamp', 'original_loan_amount']).dropDuplicates()
ln_port_ye_2018 = ln_port_ye_2018.join(raw_ln_port_ye_2018, ['account_key', 'utc_timestamp'], 'left')
ln_port_ye_2018 = ln_port_ye_2018.where(ln_port_ye_2018.gl_type.startswith('Retail'))
ln_port_ye_2018 = ln_port_ye_2018.select(['account_key', 'utc_timestamp', 'boarding_date', 'original_loan_amount', 'bank_owned_principle_assets'])

# get 2018 ml portfolio
ml_port_ye_2018 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2018/*/*/*.parquet')
ml_port_ye_2018 = ml_port_ye_2018.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ml_port_ye_2018 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2018/*/*/*.parquet')
raw_ml_port_ye_2018 = raw_ml_port_ye_2018.select(['account_key', 'utc_timestamp', 'orig_principal_balance'])
ml_port_ye_2018 = ml_port_ye_2018.join(raw_ml_port_ye_2018, ['account_key', 'utc_timestamp'], 'left')
ml_port_ye_2018 = ml_port_ye_2018.where(ml_port_ye_2018.gl_type.startswith('Retail'))
ml_port_ye_2018 = ml_port_ye_2018.select(['account_key', 'utc_timestamp', 'boarding_date', 'orig_principal_balance', 'bank_owned_principle_assets'])

# union ln and ml together
ye_2018 = ln_port_ye_2018.union(ml_port_ye_2018)


# get 2019 ln/ml portfolio:
# get 2019 ln portfolio
ln_port_ye_2019 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2019/*/*/*.parquet')
ln_port_ye_2019 = ln_port_ye_2019.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ln_port_ye_2019 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2019/*/*/*.parquet')
raw_ln_port_ye_2019 = raw_ln_port_ye_2019.where(raw_ln_port_ye_2019.participation_number==0)
raw_ln_port_ye_2019 = raw_ln_port_ye_2019.select(['account_key', 'utc_timestamp', 'original_loan_amount']).dropDuplicates()
ln_port_ye_2019 = ln_port_ye_2019.join(raw_ln_port_ye_2019, ['account_key', 'utc_timestamp'], 'left')
ln_port_ye_2019 = ln_port_ye_2019.where(ln_port_ye_2019.gl_type.startswith('RET'))
ln_port_ye_2019 = ln_port_ye_2019.select(['account_key', 'utc_timestamp', 'boarding_date', 'original_loan_amount', 'bank_owned_principle_assets'])

# get 2019 ml portfolio
ml_port_ye_2019 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2019/*/*/*.parquet')
ml_port_ye_2019 = ml_port_ye_2019.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ml_port_ye_2019 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2019/*/*/*.parquet')
raw_ml_port_ye_2019 = raw_ml_port_ye_2019.select(['account_key', 'utc_timestamp', 'orig_principal_balance'])
ml_port_ye_2019 = ml_port_ye_2019.join(raw_ml_port_ye_2019, ['account_key', 'utc_timestamp'], 'left')
ml_port_ye_2019 = ml_port_ye_2019.where(ml_port_ye_2019.gl_type.startswith('RET'))
ml_port_ye_2019 = ml_port_ye_2019.select(['account_key', 'utc_timestamp', 'boarding_date', 'orig_principal_balance', 'bank_owned_principle_assets'])

# union ln and ml together
ye_2019 = ln_port_ye_2019.union(ml_port_ye_2019)


# get 2020 ln/ml portfolio:
# get 2020 ln portfolio
ln_port_ye_2020 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2020/*/*/*.parquet')
ln_port_ye_2020 = ln_port_ye_2020.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ln_port_ye_2020 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2020/*/*/*.parquet')
raw_ln_port_ye_2020 = raw_ln_port_ye_2020.where(raw_ln_port_ye_2020.participation_number==0)
raw_ln_port_ye_2020 = raw_ln_port_ye_2020.select(['account_key', 'utc_timestamp', 'original_loan_amount']).dropDuplicates()
ln_port_ye_2020 = ln_port_ye_2020.join(raw_ln_port_ye_2020, ['account_key', 'utc_timestamp'], 'left')
ln_port_ye_2020 = ln_port_ye_2020.where(ln_port_ye_2020.gl_type.startswith('RET'))
ln_port_ye_2020 = ln_port_ye_2020.select(['account_key', 'utc_timestamp', 'boarding_date', 'original_loan_amount', 'bank_owned_principle_assets'])

# get 2020 ml portfolio
ml_port_ye_2020 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2020/*/*/*.parquet')
ml_port_ye_2020 = ml_port_ye_2020.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ml_port_ye_2020 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2020/*/*/*.parquet')
raw_ml_port_ye_2020 = raw_ml_port_ye_2020.select(['account_key', 'utc_timestamp', 'orig_principal_balance'])
ml_port_ye_2020 = ml_port_ye_2020.join(raw_ml_port_ye_2020, ['account_key', 'utc_timestamp'], 'left')
ml_port_ye_2020 = ml_port_ye_2020.where(ml_port_ye_2020.gl_type.startswith('RET'))
ml_port_ye_2020 = ml_port_ye_2020.select(['account_key', 'utc_timestamp', 'boarding_date', 'orig_principal_balance', 'bank_owned_principle_assets'])

# union ln and ml together
ye_2020 = ln_port_ye_2020.union(ml_port_ye_2020)


# get 2021 (as of yesterday) ln/ml portfolio:
# get 2021 (as of yesterday) ln portfolio
ln_port_ye_2021 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2021/12/06/*.parquet')
ln_port_ye_2021 = ln_port_ye_2021.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ln_port_ye_2021 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2021/12/06/*.parquet')
raw_ln_port_ye_2021 = raw_ln_port_ye_2021.where(raw_ln_port_ye_2021.participation_number==0)
raw_ln_port_ye_2021 = raw_ln_port_ye_2021.select(['account_key', 'utc_timestamp', 'original_loan_amount']).dropDuplicates()
ln_port_ye_2021 = ln_port_ye_2021.join(raw_ln_port_ye_2021, ['account_key', 'utc_timestamp'], 'left')
ln_port_ye_2021 = ln_port_ye_2021.where(ln_port_ye_2021.gl_type.startswith('RET'))
ln_port_ye_2021 = ln_port_ye_2021.select(['account_key', 'utc_timestamp', 'boarding_date', 'original_loan_amount', 'bank_owned_principle_assets'])

# get 2021 ml portfolio
ml_port_ye_2021 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2021/12/06/*.parquet')
ml_port_ye_2021 = ml_port_ye_2021.select(['account_key', 'utc_timestamp', 'boarding_date', 'gl_type', 'bank_owned_principle_assets'])
raw_ml_port_ye_2021 = spark.read.option("header", True).option("inferSchema", True).parquet('/mnt/*hidden*/2021/12/06/*.parquet')
raw_ml_port_ye_2021 = raw_ml_port_ye_2021.select(['account_key', 'utc_timestamp', 'orig_principal_balance'])
ml_port_ye_2021 = ml_port_ye_2021.join(raw_ml_port_ye_2021, ['account_key', 'utc_timestamp'], 'left')
ml_port_ye_2021 = ml_port_ye_2021.where(ml_port_ye_2021.gl_type.startswith('RET'))
ml_port_ye_2021 = ml_port_ye_2021.select(['account_key', 'utc_timestamp', 'boarding_date', 'orig_principal_balance', 'bank_owned_principle_assets'])

# union ln and ml together
ye_2021 = ln_port_ye_2021.union(ml_port_ye_2021)

# COMMAND ----------

# validate created files:

ye_2016.where(ye_2016.utc_timestamp!='2016-12-31').count() + ye_2017.where(ye_2017.utc_timestamp!='2017-12-31').count() + ye_2018.where(ye_2018.utc_timestamp!='2018-12-31').count() + ye_2019.where(ye_2019.utc_timestamp!='2019-12-31').count() + ye_2020.where(ye_2020.utc_timestamp!='2020-12-31').count() + ye_2021.where(ye_2021.utc_timestamp.contains('12/06/2021%')).count()

# COMMAND ----------

# 2017 PORTFOLIO PROCESSING
# get list of acct keys for isin() filter
ye_2016_accts = ye_2016.select('account_key').distinct().rdd.flatMap(lambda x: x).collect()


# get and store beginning of year portfolio
start_2017_existing = ye_2016

beginning_existing_portfolio_2017 = start_2017_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('beginning_existing_portfolio_2017')).distinct().rdd.flatMap(lambda x: x).collect()


# get end of year portfolio for beginning of year portfolio
end_2017_existing = ye_2017[ye_2017.account_key.isin(ye_2016_accts)]

ending_existing_portfolio_2017 = end_2017_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_existing_portfolio_2017')).distinct().rdd.flatMap(lambda x: x).collect()


# get the portfolio of new accounts that opened during the year
new_2017 = ye_2017[~ye_2017.account_key.isin(ye_2016_accts)]

beginning_new_portfolio_2017 = new_2017.select('original_loan_amount').agg(sum('original_loan_amount').alias('beginning_new_portfolio_2017')).distinct().rdd.flatMap(lambda x: x).collect()

ending_new_portfolio_2017 = new_2017.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_new_portfolio_2017')).distinct().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# 2018 PORTFOLIO PROCESSING
# get list of acct keys for isin() filter
ye_2017_accts = ye_2017.select('account_key').distinct().rdd.flatMap(lambda x: x).collect()


# get and store beginning of year portfolio
start_2018_existing = ye_2017

beginning_existing_portfolio_2018 = start_2018_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('beginning_existing_portfolio_2018')).distinct().rdd.flatMap(lambda x: x).collect()


# get end of year portfolio for beginning of year portfolio
end_2018_existing = ye_2018[ye_2018.account_key.isin(ye_2017_accts)]

ending_existing_portfolio_2018 = end_2018_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_existing_portfolio_2018')).distinct().rdd.flatMap(lambda x: x).collect()


# get the portfolio of new accounts that opened during the year
new_2018 = ye_2018[~ye_2018.account_key.isin(ye_2017_accts)]

beginning_new_portfolio_2018 = new_2018.select('original_loan_amount').agg(sum('original_loan_amount').alias('beginning_new_portfolio_2018')).distinct().rdd.flatMap(lambda x: x).collect()

ending_new_portfolio_2018 = new_2018.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_new_portfolio_2018')).distinct().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# 2019 PORTFOLIO PROCESSING
# get list of acct keys for isin() filter
ye_2018_accts = ye_2018.select('account_key').distinct().rdd.flatMap(lambda x: x).collect()


# get and store beginning of year portfolio
start_2019_existing = ye_2018

beginning_existing_portfolio_2019 = start_2019_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('beginning_existing_portfolio_2019')).distinct().rdd.flatMap(lambda x: x).collect()


# get end of year portfolio for beginning of year portfolio
end_2019_existing = ye_2019[ye_2019.account_key.isin(ye_2018_accts)]

ending_existing_portfolio_2019 = end_2019_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_existing_portfolio_2019')).distinct().rdd.flatMap(lambda x: x).collect()


# get the portfolio of new accounts that opened during the year
new_2019 = ye_2019[~ye_2019.account_key.isin(ye_2018_accts)]

beginning_new_portfolio_2019 = new_2019.select('original_loan_amount').agg(sum('original_loan_amount').alias('beginning_new_portfolio_2019')).distinct().rdd.flatMap(lambda x: x).collect()

ending_new_portfolio_2019 = new_2019.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_new_portfolio_2019')).distinct().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# 2020 PORTFOLIO PROCESSING
# get list of acct keys for isin() filter
ye_2019_accts = ye_2019.select('account_key').distinct().rdd.flatMap(lambda x: x).collect()


# get and store beginning of year portfolio
start_2020_existing = ye_2019

beginning_existing_portfolio_2020 = start_2020_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('beginning_existing_portfolio_2020')).distinct().rdd.flatMap(lambda x: x).collect()


# get end of year portfolio for beginning of year portfolio
end_2020_existing = ye_2020[ye_2020.account_key.isin(ye_2019_accts)]

ending_existing_portfolio_2020 = end_2020_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_existing_portfolio_2020')).distinct().rdd.flatMap(lambda x: x).collect()


# get the portfolio of new accounts that opened during the year
new_2020 = ye_2020[~ye_2020.account_key.isin(ye_2019_accts)]

beginning_new_portfolio_2020 = new_2020.select('original_loan_amount').agg(sum('original_loan_amount').alias('beginning_new_portfolio_2020')).distinct().rdd.flatMap(lambda x: x).collect()

ending_new_portfolio_2020 = new_2020.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_new_portfolio_2020')).distinct().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# 2021 PORTFOLIO PROCESSING
# get list of acct keys for isin() filter
ye_2020_accts = ye_2020.select('account_key').distinct().rdd.flatMap(lambda x: x).collect()


# get and store beginning of year portfolio
start_2021_existing = ye_2020

beginning_existing_portfolio_2021 = start_2021_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('beginning_existing_portfolio_2021')).distinct().rdd.flatMap(lambda x: x).collect()


# get end of year portfolio for beginning of year portfolio
end_2021_existing = ye_2021[ye_2021.account_key.isin(ye_2020_accts)]

ending_existing_portfolio_2021 = end_2021_existing.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_existing_portfolio_2021')).distinct().rdd.flatMap(lambda x: x).collect()


# get the portfolio of new accounts that opened during the year
new_2021 = ye_2021[~ye_2021.account_key.isin(ye_2020_accts)]

beginning_new_portfolio_2021 = new_2021.select('original_loan_amount').agg(sum('original_loan_amount').alias('beginning_new_portfolio_2021')).distinct().rdd.flatMap(lambda x: x).collect()

ending_new_portfolio_2021 = new_2021.select('bank_owned_principle_assets').agg(sum('bank_owned_principle_assets').alias('ending_new_portfolio_2021')).distinct().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

beginning_existing_portfolio_2021

# COMMAND ----------

ending_existing_portfolio_2021

# COMMAND ----------

beginning_new_portfolio_2021

# COMMAND ----------

ending_new_portfolio_2021

# COMMAND ----------


