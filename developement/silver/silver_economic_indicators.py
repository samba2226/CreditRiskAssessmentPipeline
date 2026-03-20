import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dlt.table(
    name="silver_economic_indicators",
    comment="Cleaned economic indicators dataset with error handling"
)

@dlt.expect("valid_region", "region IS NOT NULL")
@dlt.expect("valid_year", "year IS NOT NULL")
@dlt.expect("valid_property_value", "avg_property_value >= 0")
@dlt.expect("valid_interest_rate", "avg_interest_rate >= 0")

def silver_economic_indicators():

    try:

        df = spark.table(
            "credits_catalog.bronze_credit_risk_schema.bronze_economic_indicators"
        )

        # -------------------------------
        # Remove duplicates
        # -------------------------------
        df = df.dropDuplicates()

        # -------------------------------
        # Standardize region
        # -------------------------------
        df = df.withColumn("region", upper(trim(col("region"))))

        # -------------------------------
        # Cast numeric columns
        # -------------------------------
        df = df.withColumn("avg_property_value", col("avg_property_value").cast("double")) \
               .withColumn("avg_interest_rate", col("avg_interest_rate").cast("double")) \
               .withColumn("interest_rate_spread", col("interest_rate_spread").cast("double"))

        # -------------------------------
        # Remove invalid values
        # -------------------------------
        df = df.withColumn(
            "avg_interest_rate",
            when(col("avg_interest_rate") < 0, None).otherwise(col("avg_interest_rate"))
        ).withColumn(
            "interest_rate_spread",
            when(col("interest_rate_spread") < 0, None).otherwise(col("interest_rate_spread"))
        )

        # -------------------------------
        # Window medians (safe)
        # -------------------------------
        w = Window.partitionBy()

        df = df.withColumn(
            "median_property",
            percentile_approx(col("avg_property_value"), 0.5).over(w)
        ).withColumn(
            "median_rate",
            percentile_approx(col("avg_interest_rate"), 0.5).over(w)
        ).withColumn(
            "median_spread",
            percentile_approx(col("interest_rate_spread"), 0.5).over(w)
        )

        # -------------------------------
        # Handle missing values
        # -------------------------------
        df = df.withColumn(
            "avg_property_value",
            coalesce(col("avg_property_value"), col("median_property"))
        ).withColumn(
            "avg_interest_rate",
            coalesce(col("avg_interest_rate"), col("median_rate"))
        ).withColumn(
            "interest_rate_spread",
            coalesce(col("interest_rate_spread"), col("median_spread"))
        )

        df = df.drop("median_property", "median_rate", "median_spread")

        # -------------------------------
        # Derived columns
        # -------------------------------
        df = df.withColumn(
            "interest_rate_category",
            when(col("avg_interest_rate") < 3, "LOW_RATE")
            .when(col("avg_interest_rate") < 6, "MEDIUM_RATE")
            .otherwise("HIGH_RATE")
        )

        df = df.withColumn(
            "property_value_category",
            when(col("avg_property_value") < 100000, "LOW_VALUE")
            .when(col("avg_property_value") < 500000, "MEDIUM_VALUE")
            .otherwise("HIGH_VALUE")
        )

        # -------------------------------
        # Year validation
        # -------------------------------
        df = df.withColumn(
            "year",
            when(col("year") < 1990, None).otherwise(col("year"))
        )

        # -------------------------------
        # Data Quality + Error Messages
        # -------------------------------
        df = df.withColumn(
            "data_quality_flag",
            when(col("region").isNull(), "INVALID_REGION")
            .when(col("year").isNull(), "INVALID_YEAR")
            .when(col("avg_property_value") < 0, "INVALID_PROPERTY_VALUE")
            .when(col("avg_interest_rate") < 0, "INVALID_INTEREST_RATE")
            .otherwise("VALID")
        )

        df = df.withColumn(
            "error_message",
            when(col("region").isNull(), "Region is missing")
            .when(col("year").isNull(), "Year is invalid")
            .when(col("avg_property_value") < 0, "Negative property value")
            .when(col("avg_interest_rate") < 0, "Negative interest rate")
            .otherwise(None)
        )

        # -------------------------------
        # Metadata
        # -------------------------------
        df = df.withColumn("processing_timestamp", current_timestamp()) \
               .withColumn("data_source", lit("economic_indicators"))

        return df

    except Exception as e:

        # -------------------------------
        # Fallback (pipeline-safe)
        # -------------------------------
        return spark.createDataFrame(
            [(None, None, None, None, None, "PIPELINE_ERROR", str(e), current_timestamp())],
            [
                "region",
                "avg_property_value",
                "avg_interest_rate",
                "interest_rate_spread",
                "year",
                "data_quality_flag",
                "error_message",
                "processing_timestamp"
            ]
        )