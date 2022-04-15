# Import modules
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F

# Set variables
BUCKET = 'dl-eu-pub-tender'
OUTPUT = 'eu-pub-tender.results'

# Set spark parameters
spark = SparkSession.builder \
    .appName('data_processing') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', BUCKET )

# BUild schema for csv data import. Use Pandas to get ppreliminary schema and update types as needed
schema = types.StructType([
    types.StructField ("tender_row_nr", types.IntegerType(), True),          
    types.StructField('tender_id', types.StringType(), True),
    types.StructField('tender_country', types.StringType(), True),
    types.StructField('tender_title', types.StringType(), True),
    types.StructField('tender_size', types.StringType(), True),
    types.StructField('tender_supplyType', types.StringType(), True),
    types.StructField('tender_procedureType', types.StringType(), True),
    types.StructField('tender_nationalProcedureType', types.StringType(), True),
    types.StructField('tender_mainCpv', types.StringType(), True),
    types.StructField('tender_cpvs', types.StringType(), True),
    types.StructField('tender_addressOfImplementation_nuts', types.StringType(), True),
    types.StructField('tender_year', types.TimestampType(), True),
    types.StructField('tender_eligibleBidLanguages', types.StringType(), True),
    types.StructField('tender_npwp_reasons', types.StringType(), True),
    types.StructField('tender_awardDeadline', types.TimestampType(), True),
    types.StructField('tender_contractSignatureDate', types.TimestampType(), True),
    types.StructField('tender_awardDecisionDate', types.TimestampType(), True),
    types.StructField('tender_bidDeadline', types.TimestampType(), True),
    types.StructField('tender_cancellationDate', types.TimestampType(), True),
    types.StructField('tender_estimatedStartDate', types.TimestampType(), True),
    types.StructField('tender_estimatedCompletionDate', types.TimestampType(), True),
    types.StructField('tender_estimatedDurationInYears', types.IntegerType(), True),
    types.StructField('tender_estimatedDurationInMonths', types.IntegerType(), True),
    types.StructField('tender_estimatedDurationInDays', types.IntegerType(), True),
    types.StructField('tender_isEUFunded', types.StringType(), True),
    types.StructField('tender_isDps', types.StringType(), True),
    types.StructField('tender_isElectronicAuction', types.StringType(), True),
    types.StructField('tender_isAwarded', types.StringType(), True),
    types.StructField('tender_isCentralProcurement', types.StringType(), True),
    types.StructField('tender_isJointProcurement', types.StringType(), True),
    types.StructField('tender_isOnBehalfOf', types.StringType(), True),
    types.StructField('tender_isFrameworkAgreement', types.StringType(), True),
    types.StructField('tender_isCoveredByGpa', types.StringType(), True),
    types.StructField('tender_hasLots', types.StringType(), True),
    types.StructField('tender_estimatedPrice', types.FloatType(), True),
    types.StructField('tender_estimatedPrice_currency', types.StringType(), True),
    types.StructField('tender_estimatedPrice_minNetAmount', types.FloatType(), True),
    types.StructField('tender_estimatedPrice_maxNetAmount', types.FloatType(), True),
    types.StructField('tender_estimatedPrice_EUR', types.FloatType(), True),
    types.StructField('tender_finalPrice', types.FloatType(), True),
    types.StructField('tender_finalPrice_currency', types.StringType(), True),
    types.StructField('tender_finalPrice_minNetAmount', types.FloatType(), True),
    types.StructField('tender_finalPrice_maxNetAmount', types.FloatType(), True),
    types.StructField('tender_finalPrice_EUR', types.FloatType(), True),
    types.StructField('tender_description_length', types.StringType(), True),
    types.StructField('tender_personalRequirements_length', types.StringType(), True),
    types.StructField('tender_economicRequirements_length', types.StringType(), True),
    types.StructField('tender_technicalRequirements_length', types.StringType(), True),
    types.StructField('tender_documents_count', types.IntegerType(), True),
    types.StructField('tender_awardCriteria_count', types.IntegerType(), True),
    types.StructField('tender_corrections_count', types.IntegerType(), True),
    types.StructField('tender_onBehalfOf_count', types.IntegerType(), True),
    types.StructField('tender_lots_count', types.IntegerType(), True),
    types.StructField('tender_publications_count', types.IntegerType(), True),
    types.StructField('tender_publications_firstCallForTenderDate', types.TimestampType(), True),
    types.StructField('tender_publications_lastCallForTenderDate', types.TimestampType(), True),
    types.StructField('tender_publications_firstdContractAwardDate', types.TimestampType(), True),
    types.StructField('tender_publications_lastContractAwardDate', types.TimestampType(), True),
    types.StructField('tender_publications_lastContractAwardUrl', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_SINGLE_BID', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_CALL_FOR_TENDER_PUBLICATION', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_ADVERTISEMENT_PERIOD', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_PROCEDURE_TYPE', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_DECISION_PERIOD', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_TAX_HAVEN', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_NEW_COMPANY', types.StringType(), True),
    types.StructField('tender_indicator_ADMINISTRATIVE_CENTRALIZED_PROCUREMENT', types.StringType(), True),
    types.StructField('tender_indicator_ADMINISTRATIVE_ELECTRONIC_AUCTION', types.StringType(), True),
    types.StructField('tender_indicator_ADMINISTRATIVE_COVERED_BY_GPA', types.StringType(), True),
    types.StructField('tender_indicator_ADMINISTRATIVE_FRAMEWORK_AGREEMENT', types.StringType(), True),
    types.StructField('tender_indicator_ADMINISTRATIVE_ENGLISH_AS_FOREIGN_LANGUAGE', types.StringType(), True),
    types.StructField('tender_indicator_ADMINISTRATIVE_NOTICE_AND_AWARD_DISCREPANCIES', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_NUMBER_OF_KEY_MISSING_FIELDS', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_AWARD_DATE_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_BUYER_NAME_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_PROC_METHOD_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_BUYER_LOC_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_BIDDER_ID_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_BIDDER_NAME_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MARKET_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_TITLE_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_VALUE_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_YEAR_MISSING', types.StringType(), True),
    types.StructField('tender_indicator_INTEGRITY_WINNER_CA_SHARE', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_ADDRESS_OF_IMPLEMENTATION_NUTS', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_ELIGIBLE_BID_LANGUAGES', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_AWARD_CRITERIA', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_CPVS', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_DURATION_INFO', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_FUNDINGS_INFO', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_SELECTION_METHOD', types.StringType(), True),
    types.StructField('tender_indicator_TRANSPARENCY_MISSING_SUBCONTRACTED_INFO', types.StringType(), True),
    types.StructField('buyer_row_nr', types.StringType(), True),
    types.StructField('buyer_buyerType', types.StringType(), True),
    types.StructField('buyer_mainActivities', types.StringType(), True),
    types.StructField('buyer_id', types.StringType(), True),
    types.StructField('buyer_name', types.StringType(), True),
    types.StructField('buyer_nuts', types.StringType(), True),
    types.StructField('buyer_city', types.StringType(), True),
    types.StructField('buyer_country', types.StringType(), True),
    types.StructField('buyer_postcode', types.StringType(), True),
    types.StructField('lot_row_nr', types.StringType(), True),
    types.StructField('lot_title', types.StringType(), True),
    types.StructField('lot_selectionMethod', types.StringType(), True),
    types.StructField('lot_status', types.StringType(), True),
    types.StructField('lot_contractSignatureDate', types.StringType(), True),
    types.StructField('lot_cancellationDate', types.StringType(), True),
    types.StructField('lot_isAwarded', types.StringType(), True),
    types.StructField('lot_estimatedPrice', types.FloatType(), True),
    types.StructField('lot_estimatedPrice_currency', types.StringType(), True),
    types.StructField('lot_estimatedPrice_minNetAmount', types.FloatType(), True),
    types.StructField('lot_estimatedPrice_maxNetAmount', types.FloatType(), True),
    types.StructField('lot_estimatedPrice_EUR', types.FloatType(), True),
    types.StructField('lot_lotNumber', types.StringType(), True),
    types.StructField('lot_bidsCount', types.IntegerType(), True),
    types.StructField('lot_validBidsCount', types.IntegerType(), True),
    types.StructField('lot_smeBidsCount', types.IntegerType(), True),
    types.StructField('lot_electronicBidsCount', types.IntegerType(), True),
    types.StructField('lot_nonEuMemberStatesCompaniesBidsCount', types.IntegerType(), True),
    types.StructField('lot_otherEuMemberStatesCompaniesBidsCount', types.IntegerType(), True),
    types.StructField('lot_foreignCompaniesBidsCount', types.IntegerType(), True),
    types.StructField('lot_description_length', types.StringType(), True),
    types.StructField('bid_row_nr', types.StringType(), True),
    types.StructField('bid_isWinning', types.StringType(), True),
    types.StructField('bid_isSubcontracted', types.StringType(), True),
    types.StructField('bid_isConsortium', types.StringType(), True),
    types.StructField('bid_price', types.FloatType(), True),
    types.StructField('bid_price_currency', types.StringType(), True),
    types.StructField('bid_price_minNetAmount', FloatType(), True),
    types.StructField('bid_price_maxNetAmount', FloatType(), True),
    types.StructField('bid_price_EUR', types.FloatType(), True),
    types.StructField('bidder_row_nr', types.StringType(), True),
    types.StructField('bidder_id', types.StringType(), True),
    types.StructField('bidder_name', types.StringType(), True),
    types.StructField('bidder_nuts', types.StringType(), True),
    types.StructField('bidder_city', types.StringType(), True),
    types.StructField('bidder_country', types.StringType(), True),
    types.StructField('bidder_postcode', types.StringType(), True)
])

# Read csv
df = spark.read \
    .option('header', 'true') \
    .option("delimiter", ";") \
    .schema(schema) \
    .csv('gs://dl-eu-pub-tender/raw_data/country_data/*')

# define boolean function to transform string to boolean values  
def  boolean_conversion(base_str):
    if base_str == 'yes':
        return True
    elif base_str == 'no':
        return False
    else:
        return f'unknown'

# Set function as user defined function (UDF)
boolean_conversion_udf = F.udf(boolean_conversion, returnType=types.BooleanType())

# Create dataframe with data that will be used
df = df \
    .withColumn('tender_date', F.to_date(df.tender_awardDecisionDate, 'yyyy-mm-dd')) \
    .withColumn('tender_year', F.year(df.tender_year)) \
    .withColumnRenamed('tender_supplyType', 'purchase_type') \
    .withColumnRenamed('tender_procedureType', 'procedure_type') \
    .withColumnRenamed('tender_finalPrice_EUR', 'final_price') \
    .withColumnRenamed('tender_awardCriteria_count', 'award_criteria_count') \
    .withColumnRenamed('buyer_buyerType', 'buyer_type') \
    .withColumnRenamed('buyer_mainActivities', 'buyers_activities') \
    .withColumn('eu_funded', boolean_conversion_udf(df.tender_isEUFunded)) \
    .select('tender_id', 'eu_funded', 'tender_year', 'tender_date', 'tender_country','buyer_name', 'buyer_type', 'buyers_activities', 'purchase_type', 'procedure_type','award_criteria_count', 'bidder_name', 'final_price' )

# Repartition df
df.repartition(48)

# Convert to parquet and save to datalake
# df.write.parquet('gs://dl-eu-pub-tender/raw_data/parquet', mode = 'overwrite')

# Create temporary table for Spark sql queries
df.createOrReplaceTempView('data')

# Create queries
# Show 15 largest public tender suppliers in EU
df_largest_suppliers = spark.sql("""
SELECT bidder_name, sum(final_price) AS value
FROM data
WHERE bidder_name IS NOT NULL
GROUP BY bidder_name
ORDER BY sum(final_price) DESC
LIMIT 15;
""")

# Show 25 sectors, where EU spends most
df_largest_sectors = spark.sql("""
SELECT buyers_activities, sum(final_price) AS value
FROM data
WHERE buyers_activities IS NOT NULL
GROUP BY buyers_activities
ORDER BY sum(final_price) DESC
LIMIT 25;
""")

# Show purchases by procedure type
df_procedures = spark.sql("""
SELECT procedure_type, sum(final_price) AS amount
FROM data
GROUP BY procedure_type
ORDER BY sum(final_price) DESC
LIMIT 12;
""")

# Show tender values by country over years
df_values_by_country = spark.sql("""
SELECT tender_country, tender_year, sum(final_price) AS value
FROM data
GROUP BY tender_country, tender_year
ORDER BY sum(final_price) DESC;
""")

# Show largest public tender ever
df_largest_tender = spark.sql("""
SELECT tender_date, tender_year, buyer_name, buyers_activities, bidder_name, final_price
FROM data
ORDER BY final_price DESC
LIMIT 1;
""")

# Show how largest buyer revenues develop over years
df_largest_bidders_revenues = spark.sql("""
SELECT bidder_name, tender_year, sum(final_price) AS revenue
FROM data
GROUP BY bidder_name, tender_year
ORDER BY sum(final_price) DESC
LIMIT 15;
""")

# Write to Big Query


df_largest_sectors.coalesce(1) \
    .write.format('bigquery') \
    .option('table', f'{OUTPUT}.largest_sectors') \
    .mode('Overwrite') \
    .save()

df_procedures.coalesce(1) \
    .write.format('bigquery') \
    .option('table', f'{OUTPUT}.procedures') \
    .mode('Overwrite') \
    .save()

df_values_by_country.coalesce(1) \
    .write.format('bigquery') \
    .option('table', f'{OUTPUT}.values_by_country') \
    .mode('Overwrite') \
    .save()

df_largest_tender.coalesce(1) \
    .write.format('bigquery') \
    .option('table', f'{OUTPUT}.largest_tender') \
    .mode('Overwrite') \
    .save()

df_largest_bidders_revenues.coalesce(1) \
    .write.format('bigquery') \
    .option('table', f'{OUTPUT}.largest_bidders_revenues') \
    .mode('Overwrite') \
    .save()



