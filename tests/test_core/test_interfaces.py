"""Tests for core interfaces."""

import pytest

from src.dih.core.interfaces import IReader, IWriter, TableDefinition


def test_table_definition_is_abstract():
    """TableDefinition cannot be instantiated directly."""
    with pytest.raises(TypeError):
        TableDefinition()  # type: ignore[abstract]


def test_reader_is_abstract():
    """IReader cannot be instantiated directly."""
    with pytest.raises(TypeError):
        IReader()  # type: ignore[abstract]


def test_writer_is_abstract():
    """IWriter cannot be instantiated directly."""
    with pytest.raises(TypeError):
        IWriter()  # type: ignore[abstract]


def test_table_definition_default_alias():
    """Test default_alias property works."""

    class TestTableDef(TableDefinition):
        @property
        def path(self) -> str:
            return "/test/path"

        @property
        def format(self) -> str:
            return "parquet"

    table_def = TestTableDef()
    assert table_def.default_alias == "TestTableDef"


def test_table_definition_options_default():
    """Test options returns empty dict by default."""

    class TestTableDef(TableDefinition):
        @property
        def path(self) -> str:
            return "/test/path"

        @property
        def format(self) -> str:
            return "parquet"

    table_def = TestTableDef()
    assert table_def.options == {}


def test_table_definition_schema_default():
    """Test schema returns None by default."""

    class TestTableDef(TableDefinition):
        @property
        def path(self) -> str:
            return "/test/path"

        @property
        def format(self) -> str:
            return "parquet"

    table_def = TestTableDef()
    assert table_def.schema is None
