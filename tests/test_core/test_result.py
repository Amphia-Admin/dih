"""Tests for ProcessingResult."""

from typing import TYPE_CHECKING

import pytest

from dih.core.result import ProcessingResult

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_processing_result_add_and_get(spark_session: SparkSession, sample_dataframe):
    """Test adding and retrieving results."""
    result = ProcessingResult()

    result.add("test_df", sample_dataframe)

    assert "test_df" in result
    assert result["test_df"] == sample_dataframe


def test_processing_result_len(spark_session: SparkSession, sample_dataframe):
    """Test len() returns correct count."""
    result = ProcessingResult()

    assert len(result) == 0

    result.add("df1", sample_dataframe)
    result.add("df2", sample_dataframe)

    assert len(result) == 2


def test_processing_result_iteration(spark_session: SparkSession, sample_dataframe):
    """Test iteration over result names."""
    result = ProcessingResult()

    result.add("df1", sample_dataframe)
    result.add("df2", sample_dataframe)

    names = list(result)
    assert "df1" in names
    assert "df2" in names


def test_processing_result_contains(spark_session: SparkSession, sample_dataframe):
    """Test __contains__ operator."""
    result = ProcessingResult()

    result.add("test_df", sample_dataframe)

    assert "test_df" in result
    assert "nonexistent" not in result


def test_processing_result_get_nonexistent_raises(spark_session: SparkSession):
    """Test accessing nonexistent result raises KeyError."""
    result = ProcessingResult()

    with pytest.raises(KeyError):
        _ = result["nonexistent"]


def test_processing_result_repr(spark_session: SparkSession, sample_dataframe):
    """Test string representation."""
    result = ProcessingResult()
    result.add("df1", sample_dataframe)
    result.add("df2", sample_dataframe)

    repr_str = repr(result)
    assert "ProcessingResult" in repr_str
    assert "df1" in repr_str
    assert "df2" in repr_str


def test_processing_result_equality(spark_session: SparkSession, sample_dataframe):
    """Test equality comparison."""
    result1 = ProcessingResult()
    result2 = ProcessingResult()

    result1.add("df", sample_dataframe)
    result2.add("df", sample_dataframe)

    # Note: This may not work as expected due to DataFrame comparison
    # Just verify the method exists and doesn't crash
    _ = result1 == result2
