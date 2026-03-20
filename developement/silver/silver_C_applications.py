import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_credit_applications",
    comment="Cleaned credit applications dataset with error handling"
)
def silver_credit_applications():

    try:
        df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_credit_applications") \
                  .withColumnRenamed("application_id", "applicant_id")

        # Remove duplicates
        df = df.dropDuplicates(["applicant_id"])

        # -----------------------------
        # Safe Casting
        # -----------------------------
        df = df.withColumn("loan_amount", col("loan_amount").cast("double")) \
               .withColumn("rate_of_interest", col("rate_of_interest").cast("double")) \
               .withColumn("interest_rate_spread", col("interest_rate_spread").cast("double")) \
               .withColumn("upfront_charges", col("upfront_charges").cast("double")) \
               .withColumn("term", col("term").cast("int")) \
               .withColumn("application_status", col("application_status").cast("int")) \
               .withColumn("year", col("year").cast("int"))

        # -----------------------------
        # Handle invalid values
        # -----------------------------
        df = df.withColumn(
            "rate_of_interest",
            when(col("rate_of_interest") < 0, None).otherwise(col("rate_of_interest"))
        ).withColumn(
            "interest_rate_spread",
            when(col("interest_rate_spread") < 0, None).otherwise(col("interest_rate_spread"))
        ).withColumn(
            "upfront_charges",
            when(col("upfront_charges") < 0, None).otherwise(col("upfront_charges"))
        )

        # -----------------------------
        # Safe Median Calculation
        # -----------------------------
        medians = df.selectExpr(
            "percentile_approx(rate_of_interest,0.5) as median_rate",
            "percentile_approx(interest_rate_spread,0.5) as median_spread",
            "percentile_approx(upfront_charges,0.5) as median_upfront",
            "percentile_approx(loan_amount,0.5) as median_loan",
            "percentile_approx(term,0.5) as median_term"
        ).collect()[0]

        # Replace NULLs with medians
        df = df.withColumn("rate_of_interest", coalesce(col("rate_of_interest"), lit(medians["median_rate"]))) \
               .withColumn("interest_rate_spread", coalesce(col("interest_rate_spread"), lit(medians["median_spread"]))) \
               .withColumn("upfront_charges", coalesce(col("upfront_charges"), lit(medians["median_upfront"]))) \
               .withColumn("loan_amount", coalesce(col("loan_amount"), lit(medians["median_loan"]))) \
               .withColumn("term", coalesce(col("term"), lit(medians["median_term"])))

        # -----------------------------
        # Mandatory field validation
        # -----------------------------
        df = df.filter(col("applicant_id").isNotNull())

        df = df.fillna({
            "year": 2019,
            "application_status": 0
        })

        # -----------------------------
        # Standardize categorical columns
        # -----------------------------
        df = df.withColumn("loan_type", upper(trim(col("loan_type")))) \
               .withColumn("loan_purpose", upper(trim(col("loan_purpose")))) \
               .withColumn("submission_of_application", upper(trim(col("submission_of_application")))) \
               .withColumn("region", upper(trim(col("region")))) \
               .withColumn("approv_in_adv", upper(trim(col("approv_in_adv")))) \
               .withColumn("loan_limit", upper(trim(col("loan_limit"))))

        # Fix blanks
        df = df.withColumn(
            "loan_limit",
            when((col("loan_limit").isNull()) | (trim(col("loan_limit")) == ""), "UNKNOWN")
            .otherwise(col("loan_limit"))
        ).withColumn(
            "approv_in_adv",
            when((col("approv_in_adv").isNull()) | (trim(col("approv_in_adv")) == ""), "UNKNOWN")
            .otherwise(col("approv_in_adv"))
        )

        # -----------------------------
        # Add missing schema columns
        # -----------------------------
        required_cols = [
            "Neg_ammortization",
            "interest_only",
            "lump_sum_payment",
            "business_or_commercial",
            "submission_of_application"
        ]

        for c in required_cols:
            if c not in df.columns:
                df = df.withColumn(c, lit("UNKNOWN"))

        # -----------------------------
        # Data Quality + Error Handling
        # -----------------------------
        df = df.withColumn(
            "data_quality_flag",
            when(col("rate_of_interest") <= 0, "INVALID_RATE")
            .when(col("loan_amount") <= 0, "INVALID_LOAN")
            .otherwise("VALID")
        )

        df = df.withColumn(
            "error_message",
            when(col("rate_of_interest") <= 0, "Interest rate invalid")
            .when(col("loan_amount") <= 0, "Loan amount invalid")
            .otherwise(None)
        )

        # Processing timestamp
        df = df.withColumn("processing_timestamp", current_timestamp())

        return df

    except Exception as e:

        # Fallback table (pipeline-safe)
        return spark.createDataFrame(
            [(None, "PIPELINE_ERROR", str(e), current_timestamp())],
            ["applicant_id", "data_quality_flag", "error_message", "processing_timestamp"]
        )