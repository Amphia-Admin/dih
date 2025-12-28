"""Pytest configuration and fixtures."""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Create a test Spark session with Delta support."""
    # Stop any existing session
    if existing := SparkSession.getActiveSession():
        existing.stop()

    # Create new session with Delta
    builder = (
        SparkSession.builder.appName("ih-Tests")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.host", "localhost")
        .config("spark.sql.shuffle.partitions", "2")  # Speed up tests
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session: SparkSession):
    """Create a sample DataFrame for testing."""
    data = [
        (1, "Alice", "2024-01-01"),
        (2, "Bob", "2024-01-01"),
        (3, "Charlie", "2024-01-02"),
    ]
    columns = ["id", "name", "date"]
    return spark_session.createDataFrame(data, columns)
