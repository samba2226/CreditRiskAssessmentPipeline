import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="silver_loan_details",
    comment="Cleaned loan details dataset with error handling"
)
def silver_loan_details():

    try:
        df = spark.table("credits_catalog.bronze_credit_risk_schema.bronze_loan_details")

        # -----------------------------
        # Remove duplicates
        # -----------------------------
        df = df.dropDuplicates(["applicant_id"])

        # -----------------------------
        # Safe Casting
        # -----------------------------
        df = df.withColumn("loan_amount", col("loan_amount").cast("double")) \
               .withColumn("loan_term", col("loan_term").cast("int")) \
               .withColumn("property_value", col("property_value").cast("double")) \
               .withColumn("loan_to_value", col("loan_to_value").cast("double"))

        # -----------------------------
        # Validation Rules
        # -----------------------------
        df = df.withColumn(
            "loan_to_value",
            when((col("loan_to_value") < 0) | (col("loan_to_value") > 100), None)
            .otherwise(col("loan_to_value"))
        )

        df = df.withColumn(
            "loan_term",
            when((col("loan_term") <= 0) | (col("loan_term") > 600), None)
            .otherwise(col("loan_term"))
        )

        # -----------------------------
        # Safe Median Calculation
        # -----------------------------
        medians = df.selectExpr(
            "percentile_approx(loan_amount,0.5) as m_loan",
            "percentile_approx(loan_term,0.5) as m_term",
            "percentile_approx(loan_to_value,0.5) as m_ltv",
            "percentile_approx(property_value,0.5) as m_property"
        ).collect()[0]

        df = df.withColumn("loan_amount", coalesce(col("loan_amount"), lit(medians["m_loan"]))) \
               .withColumn("loan_term", coalesce(col("loan_term"), lit(medians["m_term"]))) \
               .withColumn("loan_to_value", coalesce(col("loan_to_value"), lit(medians["m_ltv"]))) \
               .withColumn("property_value", coalesce(col("property_value"), lit(medians["m_property"])))

        # -----------------------------
        # Standardize categorical columns
        # -----------------------------
        df = df.withColumn("loan_type", upper(trim(col("loan_type")))) \
               .withColumn("loan_purpose", upper(trim(col("loan_purpose"))))

        if "interest_only" in df.columns:
            df = df.withColumn("interest_only", upper(trim(col("interest_only"))))

        if "neg_amortization" in df.columns:
            df = df.withColumn("neg_amortization", upper(trim(col("neg_amortization"))))

        # -----------------------------
        # Derived Features
        # -----------------------------
        df = df.withColumn(
            "loan_size_category",
            when(col("loan_amount") < 100000, "SMALL")
            .when(col("loan_amount") < 500000, "MEDIUM")
            .otherwise("LARGE")
        )

        # -----------------------------
        # Data Quality + Error Handling
        # -----------------------------
        df = df.withColumn(
            "data_quality_flag",
            when(col("loan_amount") <= 0, "INVALID_LOAN")
            .when(col("loan_term").isNull(), "INVALID_TERM")
            .when(col("loan_to_value").isNull(), "INVALID_LTV")
            .otherwise("VALID")
        )

        df = df.withColumn(
            "error_message",
            when(col("loan_amount") <= 0, "Loan amount invalid")
            .when(col("loan_term").isNull(), "Loan term invalid")
            .when(col("loan_to_value").isNull(), "Loan to value invalid")
            .otherwise(None)
        )

        # -----------------------------
        # Ensure Required Columns Exist
        # -----------------------------
        required_cols = {
            "construction_type": "UNKNOWN",
            "occupancy_type": "UNKNOWN",
            "secured_by": "UNKNOWN",
            "total_units": "UNKNOWN",
            "security_type": "UNKNOWN"
        }

        for c, default_val in required_cols.items():
            if c not in df.columns:
                df = df.withColumn(c, lit(default_val))

        # Ensure property_value exists
        if "property_value" not in df.columns:
            df = df.withColumn("property_value", lit(0.0).cast("double"))

        # -----------------------------
        # Metadata
        # -----------------------------
        df = df.withColumn("processing_timestamp", current_timestamp())

        return df

    except Exception as e:

        # -----------------------------
        # Fallback (pipeline safe)
        # -----------------------------
        return spark.createDataFrame(
            [(None, None, None, None, "PIPELINE_ERROR", str(e), current_timestamp())],
            [
                "applicant_id",
                "loan_amount",
                "loan_term",
                "loan_to_value",
                "data_quality_flag",
                "error_message",
                "processing_timestamp"
            ]
        )