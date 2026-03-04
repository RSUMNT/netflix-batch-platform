import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Creates a local Spark session for testing without needing AWS/Iceberg."""
    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-local-testing") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()