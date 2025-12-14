"""Core abstractions and interfaces."""

from dih.core.interfaces import (
    IReader,
    IWriter,
    TableDefinition,
    TargetTableDefMixin,
)
from dih.core.reader_registry import ReaderRegistry, register_reader
from dih.core.result import ProcessingResult
from dih.core.runner import Runner
from dih.core.transformation import Transformation, transformation_definition
from dih.core.writer_registry import WriterRegistry, register_writer

__all__ = [
    "IReader",
    "IWriter",
    "TableDefinition",
    "TargetTableDefMixin",
    "ReaderRegistry",
    "WriterRegistry",
    "register_reader",
    "register_writer",
    "Transformation",
    "transformation_definition",
    "ProcessingResult",
    "Runner",
]
