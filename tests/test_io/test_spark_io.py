"""Tests for Spark reader and writer."""

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.dih.core.interfaces import TableDefinition, TargetTableDefMixin
from src.dih.io.spark_reader import SparkDataFrameReader
from src.dih.io.spark_writer import SparkDataFrameWriter

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestSourceDef(TableDefinition):
    """Test source table definition."""

    def __init__(self, path: str, file_format: str = "parquet"):
        self._path = path
        self._format = file_format

    @property
    def path(self) -> str:
        return self._path

    @property
    def format(self) -> str:
        return self._format


class TestTargetDef(TableDefinition, TargetTableDefMixin):
    """Test target table definition."""

    def __init__(
        self,
        path: str,
        file_format: str = "parquet",
        partition_by: list[str] | None = None,
        write_mode: str = "overwrite",
    ):
        self._path = path
        self._format = file_format
        self._partition_by = partition_by
        self._write_mode = write_mode

    @property
    def path(self) -> str:
        return self._path

    @property
    def format(self) -> str:
        return self._format

    @property
    def partition_by(self) -> list[str] | None:
        return self._partition_by

    @property
    def write_mode(self) -> str:
        return self._write_mode


def test_spark_reader_read_parquet(spark_session: SparkSession, sample_dataframe):
    """Test reading parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "test.parquet")

        # Write test data
        sample_dataframe.write.parquet(path)

        # Read with SparkDataFrameReader
        reader = SparkDataFrameReader()
        table_def = TestSourceDef(path, "parquet")
        df = reader.read(table_def)

        assert df.count() == 3
        assert reader.data.count() == 3


def test_spark_reader_requires_active_session():
    """Test reader raises error without active Spark session."""
    # This test assumes no session is active
    # In our test suite, session is always active, so skip this test
    pytest.skip("Cannot test without session in test environment")


def test_spark_reader_data_before_read_raises():
    """Test accessing data before read() raises error."""
    reader = SparkDataFrameReader()

    with pytest.raises(RuntimeError, match="No data loaded"):
        _ = reader.data


def test_spark_writer_write_parquet(spark_session: SparkSession, sample_dataframe):
    """Test writing parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "output.parquet")

        # Write with SparkDataFrameWriter
        writer = SparkDataFrameWriter()
        table_def = TestTargetDef(path, "parquet")
        writer.write(sample_dataframe, table_def)

        # Verify written data
        result_df = spark_session.read.parquet(path)
        assert result_df.count() == 3


def test_spark_writer_with_partitioning(spark_session: SparkSession, sample_dataframe):
    """Test writer respects partition_by."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "partitioned")

        # Write with partitioning
        writer = SparkDataFrameWriter()
        table_def = TestTargetDef(path, "parquet", partition_by=["date"])
        writer.write(sample_dataframe, table_def)

        # Verify partitions exist
        partition_path = Path(path) / "date=2024-01-01"
        assert partition_path.exists()


def test_spark_writer_csv(spark_session: SparkSession, sample_dataframe):
    """Test writing CSV files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "output.csv")

        # Write with CSV format
        writer = SparkDataFrameWriter()
        table_def = TestTargetDef(path, "csv")
        writer.write(sample_dataframe, table_def)

        # Verify written data
        result_df = spark_session.read.csv(path, header=True, inferSchema=True)
        assert result_df.count() == 3


def test_spark_reader_writer_round_trip(spark_session: SparkSession, sample_dataframe):
    """Test write then read round trip."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = str(Path(tmpdir) / "roundtrip.parquet")

        # Write
        writer = SparkDataFrameWriter()
        write_def = TestTargetDef(path, "parquet")
        writer.write(sample_dataframe, write_def)

        # Read
        reader = SparkDataFrameReader()
        read_def = TestSourceDef(path, "parquet")
        result_df = reader.read(read_def)

        # Verify
        assert result_df.count() == sample_dataframe.count()
        assert sorted(result_df.columns) == sorted(sample_dataframe.columns)
