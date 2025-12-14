"""Core abstractions and interfaces."""

from src.dih.core.interfaces import (
    IReader,
    IWriter,
    TableDefinition,
    TargetTableDefMixin,
)
from src.dih.core.reader_registry import ReaderRegistry, register_reader
from src.dih.core.result import ProcessingResult
from src.dih.core.runner import Runner
from src.dih.core.transformation import Transformation, transformation_definition
from src.dih.core.writer_registry import WriterRegistry, register_writer

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
