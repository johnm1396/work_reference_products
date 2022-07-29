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
today = "".join(str(x) for x in year + "/" + month + "/" + day)

# COMMAND ----------

# read in booked cp loans data from core and logically order columns of interest
cp_loans = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/{today}/*.parquet').select('cif_key', 'account_key', 'product_skey', 'officer_code_1', 'branch_number', 'boarding_date', 'current_note_date', 'current_maturity_date', 'non-accrual_date', 'charge_off_date', 'closed_date', 'last_payment_date', 'current_loan_amount', 'current_principal_balance', 'interest_earned_ltd', 'interest_paid_prev_ytd', 'interest_paid_ytd', 'late_charges_assessed_ltd', 'late_charges_paid_ltd', 'late_charges_waived_ltd', 'nbr_days_past_due', 'past_due_interest', 'past_due_principal', 'total_past_due', 'payoff_amount', 'utc_timestamp').orderBy('utc_timestamp', ascending=False).dropDuplicates(['account_key'])

cp_loans = cp_loans.where(cp_loans.product_skey == '*hidden*')
cp_loans = cp_loans.withColumn('cif_key', cp_loans.cif_key.cast(IntegerType()))

# COMMAND ----------

# read in campaign results from *hidden*
cp_campaign_results = spark.read.option("header", True).option("inferSchema", True).parquet(f'/mnt/*hidden*/*/*/*/*.parquet')

# combine all cp_model_groups to have one unique field per row
cp_campaign_results = cp_campaign_results.withColumn('cp_model_group', when(length(cp_campaign_results['*hidden*PredictiveModel-DigitalAds|*hidden*|b6l1cpdcv7-cp_model_group'])>0, cp_campaign_results['*hidden*PredictiveModel-DigitalAds|*hidden*|b6l1cpdcv7-cp_model_group']).otherwise(when(length(cp_campaign_results['*hidden*PredictiveModel-DigitalEmail|*hidden*|b6l1cpdcv7-cp_model_group'])>0, cp_campaign_results['*hidden*PredictiveModel-DigitalEmail|*hidden*|b6l1cpdcv7-cp_model_group']).otherwise(when(length(cp_campaign_results['*hidden*PredictiveModel-Non-DigitalEmail|*hidden*|b6l1cpdcv7-cp_model_group'])>0, cp_campaign_results['*hidden*PredictiveModel-Non-DigitalEmail|*hidden*|b6l1cpdcv7-cp_model_group']).otherwise('ERROR'))))

# combine all upload_date's to have one unique field per row
cp_campaign_results = cp_campaign_results.withColumn('upload_date', when(length(cp_campaign_results['*hidden*PredictiveModel-DigitalAds|*hidden*|b6l1cpdcv7-upload_date'])>0, cp_campaign_results['*hidden*PredictiveModel-DigitalAds|*hidden*|b6l1cpdcv7-upload_date']).otherwise(when(length(cp_campaign_results['*hidden*PredictiveModel-DigitalEmail|*hidden*|b6l1cpdcv7-upload_date'])>0, cp_campaign_results['*hidden*PredictiveModel-DigitalEmail|*hidden*|b6l1cpdcv7-upload_date']).otherwise(when(length(cp_campaign_results['*hidden*PredictiveModel-Non-DigitalEmail|*hidden*|b6l1cpdcv7-upload_date'])>0, cp_campaign_results['*hidden*PredictiveModel-Non-DigitalEmail|*hidden*|b6l1cpdcv7-upload_date']).otherwise('ERROR'))))

# reorder columns in logical manner and drop old cp_model_group and upload_date fields
cp_campaign_results = cp_campaign_results.select('id', 'campaign-id', 'campaign-category', 'campaign-group', 'campaign-name', 'campaign-segment', 'campaign-result', 'first-contact-date', 'last-contact-date', 'first-funnel-start-date', 'last-funnel-start-date', 'first-clicked-date', 'last-clicked-date', 'first-open-date', 'last-interaction-date', 'last-result', 'last-result-date', 'result-channel', 'failed-cause', 'failed-policy', 'funnel-source', 'banners-viewed', 'first-banner-viewed-date', 'last-banner-viewed-date', 'banner-result', 'popups-viewed', 'popup-result', 'push-notification-sent', 'push-notification-result', 'mails-sent', 'mail-result', 'unsubscribe-reason', 'unsubscribe-reason-other', 'trigger-personalization', 'trigger-placeholder', 'cp_model_group', 'upload_date', 'utc_timestamp')

cp_campaign_results = cp_campaign_results.withColumn('banners-viewed', cp_campaign_results['banners-viewed'].cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate first_marketing_date & first_marketing_open 

# COMMAND ----------

# select needed columns, create view_date for row depending on which date is populated
cp_campaign_contact = cp_campaign_results.select('id', 'first-contact-date', 'first-clicked-date', 'first-banner-viewed-date')
cp_campaign_contact = cp_campaign_contact.withColumn('view_date', when(cp_campaign_contact['first-clicked-date'] >= date(1901,1,1), cp_campaign_contact['first-clicked-date']).otherwise(cp_campaign_contact['first-banner-viewed-date']))

# cast first-contact-date and view_date as datetype()
cp_campaign_contact = cp_campaign_contact.withColumn('first-contact-date', cp_campaign_contact['first-contact-date'].cast(DateType()))
cp_campaign_contact = cp_campaign_contact.withColumn('view_date', cp_campaign_contact.view_date.cast(DateType()))

# calculate first_marketing_date and first_marketing_open dates for each customer_key (id)
cp_campaign_contact = cp_campaign_contact.groupBy('id').agg(min('first-contact-date').alias('first_marketing_date'), min('view_date').alias('first_marketing_open'))

# COMMAND ----------

# join cp_campaign_contact dates into cp_loans and cast boarding_date as datetype()
cp_loans = cp_loans.join(cp_campaign_contact, cp_loans.cif_key == cp_campaign_contact.id, how='left').select('cif_key', 'account_key', 'officer_code_1', 'branch_number', 'current_note_date', 'non-accrual_date', 'charge_off_date', 'closed_date', 'current_loan_amount', 'interest_earned_ltd', 'late_charges_assessed_ltd', 'late_charges_waived_ltd', 'late_charges_paid_ltd', 'nbr_days_past_due', 'past_due_principal', 'past_due_interest', 'total_past_due', 'first_marketing_date', 'first_marketing_open', 'interest_paid_prev_ytd', 'interest_paid_ytd')

cp_loans = cp_loans.withColumn('current_note_date', cp_loans.current_note_date.cast(DateType()))

# calculate flags to indicate if loan is booked after the two joined dates.
cp_loans = cp_loans.withColumn('loan_booked_after_first_marketing_date', when(cp_loans.current_note_date >= cp_loans.first_marketing_date, 'Yes').otherwise('No'))
cp_loans = cp_loans.withColumn('loan_booked_after_first_marketing_open', when(cp_loans.current_note_date >= cp_loans.first_marketing_open, 'Yes').otherwise('No'))

# COMMAND ----------

# MAGIC %md
# MAGIC Attribution Calculation

# COMMAND ----------

attribution = cp_campaign_results.join(cp_loans, cp_campaign_results.id == cp_loans.cif_key, 'left')

attribution = attribution.where(attribution.cif_key.isNotNull())

attribution = attribution.where(attribution['loan_booked_after_first_marketing_open']=='Yes')
attribution = attribution.select('account_key', 'current_note_date', 'cp_model_group', 'result-channel', 'last-result-date')

attribution = attribution.withColumn('last-result-date', attribution['last-result-date'].cast(DateType()))
attribution = attribution.withColumn('current_note_date', attribution.current_note_date.cast(DateType()))

attribution = attribution.where(attribution['last-result-date'] <= attribution.current_note_date)

attribution_window = Window.partitionBy(['account_key']).orderBy(col('last-result-date').desc())
attribution = attribution.withColumn('row', row_number().over(attribution_window)).filter(col('row') == 1).drop('row')

attribution = attribution.select('account_key', 'result-channel', 'cp_model_group').dropDuplicates()

attribution = attribution.withColumnRenamed('result-channel', 'attribution_channel')
attribution = attribution.withColumnRenamed('cp_model_group', 'attribution_model_group')

# COMMAND ----------

# calculate interest paid total
cp_loans = cp_loans.withColumn('interest_paid_total', cp_loans.interest_paid_prev_ytd + cp_loans.interest_paid_ytd)

# add attribution columns
cp_loans = cp_loans.join(attribution, 'account_key', 'left')

cp_loans = cp_loans.select('cif_key', 'account_key', 'officer_code_1', 'branch_number', 'current_note_date', 'non-accrual_date', 'charge_off_date', 'closed_date', 'current_loan_amount', 'interest_earned_ltd', 'interest_paid_total', 'late_charges_assessed_ltd', 'late_charges_waived_ltd', 'late_charges_paid_ltd', 'nbr_days_past_due', 'past_due_principal', 'past_due_interest', 'total_past_due', 'loan_booked_after_first_marketing_date', 'loan_booked_after_first_marketing_open', 'attribution_channel', 'attribution_model_group')

# COMMAND ----------

# write to delta table
cp_loans.write.format('delta').mode('overwrite').saveAsTable('*hidden*')

# COMMAND ----------

# MAGIC %md
# MAGIC Campaign Results modifications.

# COMMAND ----------

# calc banner yes/no for id
cp_campaign_banner = cp_campaign_results.groupBy('id').agg(sum('banners-viewed').alias('banner_received'))
cp_campaign_banner = cp_campaign_banner.withColumn('banner_received', when(cp_campaign_banner.banner_received > 0, 'yes').otherwise('no'))

# calc mail yes/no for id
cp_campaign_mail = cp_campaign_results.select('id', 'mail-result')
cp_campaign_mail = cp_campaign_mail.withColumn('mail_received', when(cp_campaign_mail['mail-result'].isin([':no-contact', ':failed', ':reached']), 0).otherwise(1))
cp_campaign_mail = cp_campaign_mail.groupBy('id').agg(sum('mail_received').alias('mail_received'))
cp_campaign_mail = cp_campaign_mail.withColumn('mail_received', when(cp_campaign_mail.mail_received > 0, 'yes').otherwise('no'))

# join banner and mail flags as well as the contact dates into cp_campaign_results
cp_campaign_results = cp_campaign_results.join(cp_campaign_banner, on='id', how='left')
cp_campaign_results = cp_campaign_results.join(cp_campaign_mail, on='id', how='left')
cp_campaign_results = cp_campaign_results.join(cp_campaign_contact, on='id', how='left')

# best and worst result calculations
cp_campaign_ranks = cp_campaign_results.select('id', 'campaign-result')

dict = {':unsubscribed':'0',':dismissed':'1',':no-contact':'2',':failed':'3',':reached':'4',':engaged':'5',':lead':'6',':converted':'7'}
cp_campaign_ranks = cp_campaign_ranks.na.replace(dict, subset=['campaign-result'])

cp_campaign_best = cp_campaign_ranks.select('id', 'campaign-result').withColumnRenamed('campaign-result', 'campaign_result_best').orderBy('campaign_result_best', ascending=False).dropDuplicates(subset=['id'])

cp_campaign_worst = cp_campaign_ranks.select('id', 'campaign-result').withColumnRenamed('campaign-result', 'campaign_result_worst').orderBy('campaign_result_worst', ascending=True).dropDuplicates(subset=['id'])

cp_campaign_ranks = cp_campaign_best.join(cp_campaign_worst, on='id', how='left')

# join best and worst result into cp_campaign_results
cp_campaign_results = cp_campaign_results.join(cp_campaign_ranks, on='id', how='left')

# remap best and worst results for readability
dict = {'0':'unsubscribed', '1':'dismissed', '2':'no-contact', '3':'failed', '4':'reached', '5':'engaged', '6':'lead', '7':'converted'}
cp_campaign_results = cp_campaign_results.na.replace(dict, subset=['campaign_result_best', 'campaign_result_worst'])

# calc what type of marketing was received
cp_campaign_results = cp_campaign_results.withColumn('marketing_received', when((cp_campaign_results.banner_received == 'no') & (cp_campaign_results.mail_received == 'no'), 'none').otherwise(when((cp_campaign_results.banner_received == 'yes') & (cp_campaign_results.mail_received == 'no'), 'banner only').otherwise(when((cp_campaign_results.banner_received == 'no') & (cp_campaign_results.mail_received == 'yes'), 'mail only').otherwise(when((cp_campaign_results.banner_received == 'yes') & (cp_campaign_results.mail_received == 'yes'), 'banner and mail').otherwise('error')))))

# select relevant columns
cp_campaign_results = cp_campaign_results.select('id', 'campaign-group', 'campaign-segment', 'campaign-result', 'first-contact-date', 'last-contact-date', 'first-funnel-start-date', 'last-funnel-start-date', 'first-clicked-date', 'last-clicked-date', 'first-open-date', 'last-interaction-date', 'last-result', 'last-result-date', 'result-channel', 'failed-cause', 'failed-policy', 'banners-viewed', 'first-banner-viewed-date', 'last-banner-viewed-date', 'banner-result', 'mails-sent', 'mail-result', 'unsubscribe-reason', 'unsubscribe-reason-other', 'trigger-personalization', 'trigger-placeholder', 'cp_model_group', 'upload_date', 'banner_received', 'mail_received', 'first_marketing_date', 'first_marketing_open', 'campaign_result_best', 'campaign_result_worst', 'marketing_received', 'utc_timestamp')

# cast dates
cp_campaign_results = cp_campaign_results.withColumn('first-contact-date', cp_campaign_results['first-contact-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('last-contact-date', cp_campaign_results['last-contact-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('first-funnel-start-date', cp_campaign_results['first-funnel-start-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('last-funnel-start-date', cp_campaign_results['last-funnel-start-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('first-clicked-date', cp_campaign_results['first-clicked-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('last-clicked-date', cp_campaign_results['last-clicked-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('first-open-date', cp_campaign_results['first-open-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('last-interaction-date', cp_campaign_results['last-interaction-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('last-result-date', cp_campaign_results['last-result-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('first-banner-viewed-date', cp_campaign_results['first-banner-viewed-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('last-banner-viewed-date', cp_campaign_results['last-banner-viewed-date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('upload_date', cp_campaign_results['upload_date'].cast(DateType()))
cp_campaign_results = cp_campaign_results.withColumn('utc_timestamp', cp_campaign_results['utc_timestamp'].cast(DateType()))

# COMMAND ----------

# write to delta table
cp_campaign_results.write.format('delta').mode('overwrite').saveAsTable('*hidden*')

# COMMAND ----------


