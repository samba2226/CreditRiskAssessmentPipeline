import dlt
from pyspark.sql.functions import current_timestamp, lit

# -----------------------------------------
# Source Paths
# -----------------------------------------

paths = {
    "applicant_profiles": "s3://credit-risk-ass-target-bucket/applicant_profiles/",
    "credit_applications": "s3://credit-risk-ass-target-bucket/credit_applications/",
    "credit_history": "s3://credit-risk-ass-target-bucket/credit_history/",
    "economic_indicators": "s3://credit-risk-ass-target-bucket/economic_indicators/",
    "loan_details": "s3://credit-risk-ass-target-bucket/loan_details/"
}

# -----------------------------------------
# Applicant Profiles Bronze
# -----------------------------------------

@dlt.table(
    name="bronze_applicant_profiles",
    comment="Raw applicant profiles data from S3"
)
def applicant_profiles_bronze():

    df = spark.read.format("parquet").load(paths["applicant_profiles"])

    return df.withColumn("source_path", lit(paths["applicant_profiles"])) \
             .withColumn("ingestion_time", current_timestamp())


# -----------------------------------------
# Credit Applications Bronze
# -----------------------------------------

@dlt.table(
    name="bronze_credit_applications",
    comment="Raw credit applications data from S3"
)
def credit_applications_bronze():

    df = spark.read.format("parquet").load(paths["credit_applications"])

    return df.withColumn("source_path", lit(paths["credit_applications"])) \
             .withColumn("ingestion_time", current_timestamp())


# -----------------------------------------
# Credit History Bronze
# -----------------------------------------

@dlt.table(
    name="bronze_credit_history",
    comment="Raw credit history data from S3"
)
def credit_history_bronze():

    df = spark.read.format("parquet").load(paths["credit_history"])

    return df.withColumn("source_path", lit(paths["credit_history"])) \
             .withColumn("ingestion_time", current_timestamp())


# -----------------------------------------
# Economic Indicators Bronze
# -----------------------------------------

@dlt.table(
    name="bronze_economic_indicators",
    comment="Raw economic indicators data from S3"
)
def economic_indicators_bronze():

    df = spark.read.format("parquet").load(paths["economic_indicators"])

    return df.withColumn("source_path", lit(paths["economic_indicators"])) \
             .withColumn("ingestion_time", current_timestamp())


# -----------------------------------------
# Loan Details Bronze
# -----------------------------------------

@dlt.table(
    name="bronze_loan_details",
    comment="Raw loan details data from S3"
)
def loan_details_bronze():

    df = spark.read.format("parquet").load(paths["loan_details"])

    return df.withColumn("source_path", lit(paths["loan_details"])) \
             .withColumn("ingestion_time", current_timestamp())