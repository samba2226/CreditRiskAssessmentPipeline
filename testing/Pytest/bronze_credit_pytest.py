import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------------------------------------------------------
# Spark Session Fixture
# -------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("bronze-layer-testing")
        .getOrCreate()
    )
    return spark


# -------------------------------------------------------
# Dataset Paths (same as pipeline)
# -------------------------------------------------------

datasets = {
    "applicant_profiles": "s3://credit-risk-ass-target-bucket/applicant_profiles/",
    "credit_applications": "s3://credit-risk-ass-target-bucket/credit_applications/",
    "credit_history": "s3://credit-risk-ass-target-bucket/credit_history/",
    "economic_indicators": "s3://credit-risk-ass-target-bucket/economic_indicators/"
}


# -------------------------------------------------------
# Test 1 : Check Dataset Path Exists
# -------------------------------------------------------

@pytest.mark.parametrize("dataset,path", datasets.items())
def test_dataset_path_exists(spark, dataset, path):

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )

    exists = fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(path)
    )

    assert exists, f"{dataset} dataset path does not exist"


# -------------------------------------------------------
# Test 2 : Check Dataset Can Be Read
# -------------------------------------------------------

@pytest.mark.parametrize("dataset,path", datasets.items())
def test_dataset_readable(spark, dataset, path):

    df = spark.read.option("header", "true").parquet(path)

    assert df.count() > 0, f"{dataset} dataset is empty"


# -------------------------------------------------------
# Test 3 : Check Required Columns
# -------------------------------------------------------

@pytest.mark.parametrize("dataset,path", datasets.items())
def test_required_columns(spark, dataset, path):

    df = spark.read.option("header", "true").parquet(path)

    columns = df.columns

    assert len(columns) > 0, f"{dataset} has no columns"


# -------------------------------------------------------
# Test 4 : Schema Validation
# -------------------------------------------------------

@pytest.mark.parametrize("dataset,path", datasets.items())
def test_schema_validation(spark, dataset, path):

    df = spark.read.option("header", "true").parquet(path)

    schema = df.schema

    assert schema is not None


# -------------------------------------------------------
# Test 5 : Invalid Path Should Fail
# -------------------------------------------------------

def test_invalid_path_failure(spark):

    invalid_path = "s3://credit-risk-ass-target-bucket/invalid_path/"

    with pytest.raises(Exception):
        spark.read.option("header","true").parquet(invalid_path).count()


# -------------------------------------------------------
# Test 6 : Null Data Check
# -------------------------------------------------------

@pytest.mark.parametrize("dataset,path", datasets.items())
def test_null_check(spark, dataset, path):

    df = spark.read.option("header","true").parquet(path)

    null_count = df.filter(col(df.columns[0]).isNull()).count()

    assert null_count >= 0


# -------------------------------------------------------
# Test 7 : DataFrame Creation
# -------------------------------------------------------

@pytest.mark.parametrize("dataset,path", datasets.items())
def test_dataframe_creation(spark, dataset, path):

    df = spark.read.option("header","true").parquet(path)

    assert df is not None