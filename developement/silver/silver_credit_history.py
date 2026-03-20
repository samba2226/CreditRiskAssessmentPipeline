import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_credit_history",
    comment="Cleaned credit history dataset with error handling"
)
def silver_credit_history():

    try:
        df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_credit_history")

        # Remove duplicates
        df = df.dropDuplicates(["applicant_id"])

        # -----------------------------
        # Safe Casting
        # -----------------------------
        df = df.withColumn("credit_score", col("credit_score").cast("int")) \
               .withColumn("debt_to_income_ratio", col("debt_to_income_ratio").cast("double"))

        # -----------------------------
        # Standardize categorical columns
        # -----------------------------
        df = df.withColumn("credit_worthiness", upper(trim(col("credit_worthiness")))) \
               .withColumn("open_credit", upper(trim(col("open_credit")))) \
               .withColumn("credit_type", upper(trim(col("credit_type")))) \
               .withColumn("co_applicant_credit_type", upper(trim(col("co_applicant_credit_type")))) \
               .withColumn("negative_amortization", upper(trim(col("negative_amortization")))) \
               .withColumn("interest_only", upper(trim(col("interest_only")))) \
               .withColumn("lump_sum_payment", upper(trim(col("lump_sum_payment"))))

        # Fix NULL / blank open_credit
        df = df.withColumn(
            "open_credit",
            when((col("open_credit").isNull()) | (trim(col("open_credit")) == ""), "UNKNOWN")
            .otherwise(col("open_credit"))
        )

        # -----------------------------
        # Data Validation
        # -----------------------------
        df = df.withColumn(
            "credit_score",
            when((col("credit_score") < 300) | (col("credit_score") > 850), None)
            .otherwise(col("credit_score"))
        )

        # -----------------------------
        # Safe Median Calculation
        # -----------------------------
        median_val = df.selectExpr(
            "percentile_approx(credit_score,0.5) as median_score"
        ).collect()[0]["median_score"]

        df = df.withColumn(
            "credit_score",
            coalesce(col("credit_score"), lit(median_val))
        )

        # Replace NULL numeric values
        df = df.fillna({
            "debt_to_income_ratio": 0
        })

        # -----------------------------
        # Feature Engineering
        # -----------------------------
        df = df.withColumn(
            "credit_score_band",
            when(col("credit_score") < 580, "POOR")
            .when(col("credit_score") <= 669, "FAIR")
            .when(col("credit_score") <= 739, "GOOD")
            .when(col("credit_score") <= 799, "VERY_GOOD")
            .otherwise("EXCELLENT")
        )

        df = df.withColumn(
            "risk_category",
            when((col("credit_score") < 600) | (col("debt_to_income_ratio") > 40), "HIGH_RISK")
            .when(col("credit_score") <= 700, "MEDIUM_RISK")
            .otherwise("LOW_RISK")
        )

        # -----------------------------
        # Data Quality + Error Handling
        # -----------------------------
        df = df.withColumn(
            "data_quality_flag",
            when(col("credit_score").isNull(), "INVALID_SCORE")
            .when(col("debt_to_income_ratio") < 0, "INVALID_DTI")
            .otherwise("VALID")
        )

        df = df.withColumn(
            "error_message",
            when(col("credit_score").isNull(), "Credit score missing or invalid")
            .when(col("debt_to_income_ratio") < 0, "DTI cannot be negative")
            .otherwise(None)
        )

        # Processing timestamp
        df = df.withColumn("processing_timestamp", current_timestamp())

        return df

    except Exception as e:

        # Pipeline-safe fallback
        return spark.createDataFrame(
            [(None, None, None, "PIPELINE_ERROR", str(e), current_timestamp())],
            ["applicant_id", "credit_score", "debt_to_income_ratio",
             "data_quality_flag", "error_message", "processing_timestamp"]
        )