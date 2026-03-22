from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# ----------------------------------------------------
# DAG Configuration
# ----------------------------------------------------

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="s3_glue_databricks_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["databricks", "dlt", "etl"]
) as dag:

    # ----------------------------------------------------
    # 1 Detect File Arrival in S3
    # ----------------------------------------------------
    s3_file_sensor = S3KeySensor(
        task_id="check_s3_file",
        bucket_name="credit-risk-ass-source-bucket",
        bucket_key="raw_dataset/*/*.csv",
        wildcard_match=True,
        aws_conn_id="aws_air",
        timeout=60 * 60,
        poke_interval=60
    )

    # ----------------------------------------------------
    # 2 Run Glue ETL Job
    # ----------------------------------------------------
    run_glue_job = GlueJobOperator(
        task_id="run_glue_etl",
        job_name="CreditRiskAssessmentJob",
        region_name="ap-south-1",
        aws_conn_id="aws_air",
        wait_for_completion=True,
	verbose=True
    )

    # ----------------------------------------------------
    # 3 Run Databricks Bronze DLT Pipeline
    # ----------------------------------------------------
    bronze_dlt_pipeline = DatabricksSubmitRunOperator(
        task_id="run_bronze_dlt_pipeline",
        databricks_conn_id="abc",
        pipeline_task={
            "pipeline_id": "eab016d0-045b-43a9-8039-11747b5cad13"
        }
    )

    # ----------------------------------------------------
    # 4 Run Databricks Silver DLT Pipeline
    # ----------------------------------------------------
    silver_dlt_pipeline = DatabricksSubmitRunOperator(
        task_id="run_silver_dlt_pipeline",
        databricks_conn_id="abc",
        pipeline_task={
            "pipeline_id": "0e59fddc-fa38-46ca-a428-ea1564536a64"
        }
    )

    # ----------------------------------------------------
    # 5 Run Databricks Gold DLT Pipeline
    # ----------------------------------------------------
    gold_dlt_pipeline = DatabricksSubmitRunOperator(
        task_id="run_gold_dlt_pipeline",
        databricks_conn_id="abc",
        pipeline_task={
            "pipeline_id": "0a70a3bc-0b1e-4106-9024-472d1bba4c5a"
        }
    )

    # ----------------------------------------------------
    # DAG Workflow
    # ----------------------------------------------------
    s3_file_sensor >> run_glue_job >> bronze_dlt_pipeline >> silver_dlt_pipeline >> gold_dlt_pipeline





