"""Tests for transformation base classes."""

from typing import TYPE_CHECKING

import pytest

from dih.core.transformation import Transformation, transformation_definition

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_transformation_has_inputs_outputs(spark_session: SparkSession, sample_dataframe):
    """Test transformation has inputs and outputs properties."""
    transform = Transformation()

    # Test inputs
    transform.inputs = {"test": sample_dataframe}
    assert "test" in transform.inputs
    assert transform.inputs["test"] == sample_dataframe

    # Test outputs
    transform.outputs.add("result", sample_dataframe)
    assert "result" in transform.outputs


def test_transformation_has_metadata():
    """Test transformation metadata property."""
    transform = Transformation()

    transform.metadata = {"run_date": "2024-01-01"}
    assert transform.metadata["run_date"] == "2024-01-01"


def test_transformation_definition_decorator():
    """Test transformation_definition decorator sets name and description."""

    @transformation_definition(name="test_transform", description="A test transformation")
    class TestTransformation(Transformation):
        pass

    assert TestTransformation.name == "test_transform"
    assert TestTransformation.description == "A test transformation"


def test_transformation_definition_decorator_invalid_class():
    """Test decorator raises error for non-Transformation class."""

    class NotATransformation:
        pass

    with pytest.raises(ValueError, match="Expected subclass of 'Transformation'"):

        @transformation_definition(name="test", description="test")
        class BadClass(NotATransformation):  # type: ignore[misc]
            pass


def test_transformation_repr():
    """Test transformation __repr__ method."""

    @transformation_definition(
        name="test", description="A short description for testing purposes"
    )
    class TestTransform(Transformation):
        pass

    transform = TestTransform()
    repr_str = repr(transform)

    assert "TestTransform" in repr_str
    assert "short description" in repr_str


def test_transformation_repr_truncates_long_description():
    """Test __repr__ truncates long descriptions."""
    long_desc = "A" * 100

    @transformation_definition(name="test", description=long_desc)
    class TestTransform(Transformation):
        pass

    transform = TestTransform()
    repr_str = repr(transform)

    assert "..." in repr_str
    assert len(repr_str) < len(long_desc) + 50  # Significantly shorter
