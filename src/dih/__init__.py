"""DIH - Data Ingestion Hub Framework."""

from dih.constants import FileFormat, LakeLayer, WriteMode
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
from dih.delta import (
    DeltaMergeAutoPartitionWriter,
    DeltaMergeWriter,
    DeltaWriterBase,
)
from dih.io.spark_reader import SparkDataFrameReader
from dih.io.spark_writer import SparkDataFrameWriter
from dih.utils.loader import DynamicLoader
from dih.utils.paths import PathBuilder, path_exists

__version__ = "0.1.0"
__all__ = [
    # Core interfaces
    "IReader",
    "IWriter",
    "TableDefinition",
    "TargetTableDefMixin",
    # Registries
    "ReaderRegistry",
    "WriterRegistry",
    "register_reader",
    "register_writer",
    # Transformation
    "Transformation",
    "transformation_definition",
    "ProcessingResult",
    # Runner
    "Runner",
    # I/O
    "SparkDataFrameReader",
    "SparkDataFrameWriter",
    # Delta Writers
    "DeltaWriterBase",
    "DeltaMergeWriter",
    "DeltaMergeAutoPartitionWriter",
    # Utilities
    "PathBuilder",
    "path_exists",
    "DynamicLoader",
    # Constants
    "FileFormat",
    "WriteMode",
    "LakeLayer",
]
