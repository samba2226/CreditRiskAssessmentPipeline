
# import dlt
# from pyspark.sql.functions import *

# @dlt.table(
#     name="risk_feature_dataset",
#     comment="Gold layer dataset for credit risk analysis"
# )
# def risk_feature_dataset():

#     # Read Silver tables with aliases
#     ap = spark.table("credits_catalog.silver_credit_risk_schema.silver_applicant_profiles").alias("ap")
#     ld = spark.table("credits_catalog.silver_credit_risk_schema.silver_loan_details").alias("ld")
#     ch = spark.table("credits_catalog.silver_credit_risk_schema.silver_credit_history").alias("ch")
#     ca = spark.table("credits_catalog.silver_credit_risk_schema.silver_credit_applications").alias("ca")
#     ei = spark.table("credits_catalog.silver_credit_risk_schema.silver_economic_indicators").alias("ei")

#     # Join applicant related tables
#     df = ap \
#         .join(ld, ap.applicant_id == ld.applicant_id, "left") \
#         .join(ch, ap.applicant_id == ch.applicant_id, "left") \
#         .join(ca, ap.applicant_id == ca.applicant_id, "left")

#     # Join economic indicators using applicant region
#     df = df.join(ei, ap.region == ei.region, "left")

#     # Select required columns to avoid ambiguity
#     df = df.select(
#         ap.applicant_id,
#         ap.age,
#         ap.income,
#         ap.region.alias("region"),

#         ld.loan_amount,
#         ld.loan_term,
#         ld.property_value,
#         ld.loan_to_value,

#         ch.credit_score,
#         ch.debt_to_income_ratio,

#         ca.rate_of_interest,

#         ei.avg_property_value,
#         ei.avg_interest_rate
#     )

#     # Feature engineering
#     df = df.withColumn(
#         "loan_to_income_ratio",
#         when(col("income") > 0, col("loan_amount") / col("income")).otherwise(0)
#     )

#     df = df.withColumn(
#         "credit_score_band",
#         when(col("credit_score") < 580, "Poor")
#         .when(col("credit_score") <= 669, "Fair")
#         .when(col("credit_score") <= 739, "Good")
#         .when(col("credit_score") <= 799, "Very Good")
#         .otherwise("Excellent")
#     )

#     df = df.withColumn(
#         "risk_category",
#         when((col("credit_score") < 600) | (col("debt_to_income_ratio") > 40), "HIGH_RISK")
#         .when(col("credit_score") <= 700, "MEDIUM_RISK")
#         .otherwise("LOW_RISK")
#     )

#     df = df.withColumn(
#         "expected_default_probability",
#         when(col("risk_category") == "HIGH_RISK", 0.35)
#         .when(col("risk_category") == "MEDIUM_RISK", 0.20)
#         .otherwise(0.05)
#     )

#     df = df.withColumn("processing_timestamp", current_timestamp())

#     return df

