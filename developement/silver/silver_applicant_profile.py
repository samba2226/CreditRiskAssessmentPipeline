import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_applicant_profiles",
    comment="Cleaned applicant profile dataset with error handling"
)
def silver_applicant_profiles():

    try:
        df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_applicant_profiles")

        # Remove duplicates
        df = df.dropDuplicates(["applicant_id"])

        # Safe casting
        df = df.withColumn("income", col("income").cast("double"))

        # Handle missing income
        df = df.fillna({
            "income": 0
        })

        # Standardize gender
        df = df.withColumn(
            "gender",
            when(col("gender") == "Sex Not Available", "UNKNOWN")
            .otherwise(upper(trim(col("gender"))))
        )

        # Standardize categorical columns
        df = df.withColumn("age", upper(trim(col("age")))) \
               .withColumn("region", upper(trim(col("region")))) \
               .withColumn("business_or_commercial", upper(trim(col("business_or_commercial")))) \
               .withColumn("occupancy_type", upper(trim(col("occupancy_type")))) \
               .withColumn("construction_type", upper(trim(col("construction_type")))) \
               .withColumn("total_units", upper(trim(col("total_units"))))

        # -----------------------------
        # Data Quality Rules
        # -----------------------------
        df = df.withColumn(
            "data_quality_flag",
            when(col("income") <= 0, "INVALID_INCOME")
            .when(col("applicant_id").isNull(), "MISSING_ID")
            .otherwise("VALID")
        )

        # -----------------------------
        # Error Message Column
        # -----------------------------
        df = df.withColumn(
            "error_message",
            when(col("income") <= 0, "Income is zero or negative")
            .when(col("applicant_id").isNull(), "Applicant ID is missing")
            .otherwise(None)
        )

        # Processing timestamp
        df = df.withColumn("processing_timestamp", current_timestamp())

        return df

    except Exception as e:

        # Fallback empty dataframe with error message
        return spark.createDataFrame(
            [(None, None, None, None, "PIPELINE_ERROR", str(e), current_timestamp())],
            ["applicant_id", "gender", "age", "income", "data_quality_flag", "error_message", "processing_timestamp"]
        )